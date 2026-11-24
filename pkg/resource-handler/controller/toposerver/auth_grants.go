package toposerver

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
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

type projectGrants struct {
	roots   topology.Roots
	grants  map[string]*etcdIdentityGrant
	cluster *api.MultigresCluster
	paths   []string
}

func (p *projectGrants) add(root string, operatorCredential bool) error {
	if !operatorCredential {
		return fmt.Errorf(
			"cannot authorize referencing cluster %s/%s: an operator-issued topology client certificate is required",
			p.cluster.Namespace,
			p.cluster.Name,
		)
	}
	p.paths = append(p.paths, root)
	return nil
}

// commit grants every added root, or none when one of them is outside the
// project prefix, and then returns why.
func (p *projectGrants) commit() (refusal string) {
	for _, root := range p.paths {
		if !strings.HasPrefix(root+"/", p.roots.KeyPrefix()) || strings.ContainsRune(root, 0) ||
			path.Clean(root) != root {
			return fmt.Sprintf(
				"has topology root %q outside project prefix %q or not canonical",
				root,
				p.roots.KeyPrefix(),
			)
		}
	}
	name := p.roots.ClusterRoot()
	key := client.ObjectKeyFromObject(p.cluster)
	for _, root := range p.paths {
		grant := p.grants[name]
		if grant == nil {
			grant = &etcdIdentityGrant{Name: name}
			p.grants[name] = grant
		}
		if !slices.Contains(grant.Clusters, key) {
			grant.Clusters = append(grant.Clusters, key)
		}
		grant.Permissions = append(grant.Permissions, etcdPrefixPermission(root+"/"))
	}
	return ""
}

// desiredEtcdGrants returns the grants every referencing cluster needs, and the
// referencing clusters it refuses.
func desiredEtcdGrants(
	ctx context.Context,
	reader client.Reader,
	ts *api.TopoServer,
) ([]etcdIdentityGrant, []clusterRefusal, error) {
	clusters, cells, err := grantDiscoveryObjects(ctx, reader, ts)
	if err != nil {
		return nil, nil, err
	}
	cellsByCluster := indexCellsByCluster(cells.Items)
	grants := map[string]*etcdIdentityGrant{}
	matchedCells := map[client.ObjectKey]bool{}
	var refused []clusterRefusal
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		key := client.ObjectKeyFromObject(cluster)
		clusterCells := cellsByCluster[key]
		for _, cell := range clusterCells {
			matchedCells[client.ObjectKeyFromObject(cell)] = true
		}

		reason, err := addClusterGrants(ctx, reader, ts, cluster, clusterCells, grants)
		if err != nil {
			return nil, nil, err
		}
		if reason != "" {
			refused = append(refused, clusterRefusal{cluster: key, reason: reason})
		}
	}
	for i := range cells.Items {
		cell := &cells.Items[i]
		if matchedCells[client.ObjectKeyFromObject(cell)] {
			continue
		}
		global := cell.Spec.GlobalTopoServer.Address
		if topology.EndpointMatchesTopoServer(ts, cell.Namespace, global) ||
			topology.ExternalMatchesTopoServer(
				ts,
				cell.Namespace,
				externalLocal(cell.Spec.TopoServer),
			) ||
			topologyOwnedBy(ts, cell) {
			return nil, nil, fmt.Errorf(
				"cannot resolve project identity for referencing cell %s/%s",
				cell.Namespace,
				cell.Name,
			)
		}
	}
	slices.SortFunc(refused, func(a, b clusterRefusal) int {
		return strings.Compare(a.cluster.String(), b.cluster.String())
	})
	return finalizeGrants(grants), refused, nil
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

// addClusterGrants adds the grants cluster needs on ts. A cluster that
// references ts without a project reference, or with a root outside its
// project prefix, gets none and comes back with the reason it is refused, so
// such a root is never granted, cached or tracked.
func addClusterGrants(
	ctx context.Context,
	reader client.Reader,
	ts *api.TopoServer,
	cluster *api.MultigresCluster,
	clusterCells []*api.Cell,
	grants map[string]*etcdIdentityGrant,
) (refusal string, err error) {
	res := resolver.NewResolver(reader, cluster.Namespace)
	global, globalErr := res.ResolveGlobalTopo(ctx, cluster)
	matchesGlobal := false
	if globalErr == nil {
		matchesGlobal = topology.ExternalMatchesTopoServer(
			ts,
			cluster.Namespace,
			global.External,
		) ||
			(global.Etcd != nil && topologyOwnedBy(ts, cluster))
	}

	referencesServer := topologyOwnedBy(ts, cluster) || matchesGlobal ||
		inlineClusterReferencesTopology(ts, cluster)
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
	for _, config := range cluster.Spec.Cells {
		if materialized[config.Name] {
			continue
		}
		config.CellTemplate = cluster.Spec.EffectiveCellTemplate(config.CellTemplate)
		_, _, local, err := res.ResolveCell(ctx, cluster, &config)
		resolvedCells = append(resolvedCells, resolvedCell{local: local, err: err})
		if err == nil &&
			topology.ExternalMatchesTopoServer(ts, cluster.Namespace, externalLocal(local)) {
			referencesServer = true
		}
	}

	// Resolution failures in unrelated clusters must not block this server.
	// Ownership or a known reference establishes association and makes
	// incomplete discovery fail closed.
	if !referencesServer {
		return "", nil
	}
	if globalErr != nil {
		return "", fmt.Errorf("resolving cluster %s topology: %w", cluster.Name, globalErr)
	}
	for _, resolved := range resolvedCells {
		if resolved.err != nil {
			return "", fmt.Errorf(
				"resolving cluster %s cell topology: %w",
				cluster.Name,
				resolved.err,
			)
		}
	}
	if !topology.HasProjectRef(cluster) {
		return "has no " + metadata.AnnotationProjectRef + " annotation", nil
	}

	roots, err := topology.ForCluster(cluster)
	if err != nil {
		return "", err
	}
	project := projectGrants{roots: roots, grants: grants, cluster: cluster}
	operatorCredential := api.OperatorIssuesTopoClientCert(cluster)
	if matchesGlobal {
		var rootPath string
		if global.Etcd != nil {
			rootPath = global.Etcd.RootPath
		} else {
			rootPath = global.External.RootPath
		}
		if err := project.add(rootPath, operatorCredential); err != nil {
			return "", err
		}
	}
	// After a move to another server, pods not yet restarted still use the root
	// this one serves with the operator-issued certificate, so the owner keeps
	// it until retirement deletes the server, whatever the new server's
	// credentials are.
	if topologyOwnedBy(ts, cluster) && ts.Spec.Etcd != nil && ts.Spec.Etcd.RootPath != "" {
		project.paths = append(project.paths, ts.Spec.Etcd.RootPath)
	}
	for _, cell := range clusterCells {
		if err := project.addCell(ts, cell); err != nil {
			return "", err
		}
	}
	for _, resolved := range resolvedCells {
		if topology.ExternalMatchesTopoServer(
			ts,
			cluster.Namespace,
			externalLocal(resolved.local),
		) {
			if err := project.add(
				resolved.local.External.RootPath,
				operatorCredential,
			); err != nil {
				return "", err
			}
		}
	}
	return project.commit(), nil
}

func grantDiscoveryObjects(
	ctx context.Context,
	reader client.Reader,
	ts *api.TopoServer,
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
	activated := ts.Status.AuthEnabled
	seenClusters := map[client.ObjectKey]bool{}
	if owner != nil {
		ownerCluster, topologyOwnerCell, err := topologyServerOwnerCluster(ctx, reader, ts, owner)
		if err != nil {
			return nil, nil, err
		}
		if ownerCluster == nil {
			return clusters, cells, nil
		}
		if activated {
			clusters.Items = append(clusters.Items, *ownerCluster)
			seenClusters[client.ObjectKeyFromObject(ownerCluster)] = true
			if err := listClusterCells(ctx, reader, ownerCluster, cells); err != nil {
				return nil, nil, err
			}
			if topologyOwnerCell != nil {
				appendCellIfMissing(cells, topologyOwnerCell)
			}
		}
	}
	if !activated {
		if err := reader.List(ctx, clusters); err != nil {
			return nil, nil, fmt.Errorf(
				"listing clusters before topology authorization activation: %w",
				err,
			)
		}
		if err := reader.List(ctx, cells); err != nil {
			return nil, nil, fmt.Errorf(
				"listing Cells before topology authorization activation: %w",
				err,
			)
		}
		return clusters, cells, nil
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
		for _, server := range topology.EndpointTopoServerKeys(namespace, string(endpoint)) {
			if !slices.Contains(servers, server) {
				servers = append(servers, server)
			}
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

// authManagedTopoServersForCluster enqueues the auth-managed servers a cluster
// points at, for a cluster that was deleted or whose project reference changed.
func (r *TopoServerReconciler) authManagedTopoServersForCluster(
	ctx context.Context,
	object client.Object,
) []reconcile.Request {
	cluster, ok := object.(*api.MultigresCluster)
	if !ok {
		return nil
	}
	return r.authManagedRequests(ctx, resolvedTopologyServers(ctx, r.Client, cluster), object)
}

// resolvedTopologyServers returns the servers a cluster's spec points at once
// its templates are resolved. A template that cannot be read contributes no
// server, which leaves it to the Cell events and the periodic resync.
func resolvedTopologyServers(
	ctx context.Context,
	reader client.Reader,
	cluster *api.MultigresCluster,
) []client.ObjectKey {
	res := resolver.NewResolver(reader, cluster.Namespace)
	var endpoints []api.EndpointUrl
	if global, err := res.ResolveGlobalTopo(ctx, cluster); err == nil &&
		global.External != nil {
		endpoints = append(endpoints, global.External.Endpoints...)
	}
	for _, config := range cluster.Spec.Cells {
		config.CellTemplate = cluster.Spec.EffectiveCellTemplate(config.CellTemplate)
		_, _, local, err := res.ResolveCell(ctx, cluster, &config)
		if err != nil {
			continue
		}
		if external := externalLocal(local); external != nil {
			endpoints = append(endpoints, external.Endpoints...)
		}
	}
	return referencedTopologyServers(cluster.Namespace, endpoints)
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

func indexCellsByCluster(cells []api.Cell) map[client.ObjectKey][]*api.Cell {
	result := make(map[client.ObjectKey][]*api.Cell)
	for i := range cells {
		cell := &cells[i]
		if key, ok := cellClusterKey(cell); ok {
			result[key] = append(result[key], cell)
		}
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
	matchesGlobal := topology.EndpointMatchesTopoServer(ts, cell.Namespace, global.Address)
	local := cell.Spec.TopoServer
	matchesLocal := topology.ExternalMatchesTopoServer(ts, cell.Namespace, externalLocal(local))
	ownsLocal := local != nil && local.Etcd != nil && topologyOwnedBy(ts, cell)
	operatorCredential := api.OperatorIssuesTopoClientCert(p.cluster)
	if global.CASecret != "" || global.ClientCertSecret != "" {
		// A Cell still on the source can carry its operator-issued Secret
		// after the parent has switched to custom credentials on the destination.
		secretName := api.TopoClientCertSecretName(p.cluster.Name)
		operatorCredential = global.CASecret == secretName &&
			global.ClientCertSecret == secretName &&
			(operatorCredential || (matchesGlobal || matchesLocal || ownsLocal) &&
				p.usedSourceCredential(ts))
	}
	if matchesGlobal {
		if err := p.add(global.RootPath, operatorCredential); err != nil {
			return err
		}
	}
	if matchesLocal {
		return p.add(local.External.RootPath, operatorCredential)
	}
	if ownsLocal {
		return p.add(local.Etcd.RootPath, operatorCredential)
	}
	return nil
}

func (p *projectGrants) usedSourceCredential(ts *api.TopoServer) bool {
	global := p.cluster.Spec.GlobalTopoServer
	if global == nil || global.External == nil ||
		topology.ExternalMatchesTopoServer(ts, p.cluster.Namespace, global.External) {
		return false
	}
	if topologyOwnedBy(ts, p.cluster) {
		return true
	}
	identity := p.roots.ClusterRoot()
	return slices.Contains(ts.Status.AuthIdentities, identity) && slices.Contains(
		strings.Split(ts.Status.AuthIdentityClusters[identity], ","),
		client.ObjectKeyFromObject(p.cluster).String(),
	)
}

func inlineClusterReferencesTopology(ts *api.TopoServer, cluster *api.MultigresCluster) bool {
	if global := cluster.Spec.GlobalTopoServer; global != nil &&
		topology.ExternalMatchesTopoServer(ts, cluster.Namespace, global.External) {
		return true
	}
	return slices.ContainsFunc(cluster.Spec.Cells, func(cell api.CellConfig) bool {
		return cell.Spec != nil && topology.ExternalMatchesTopoServer(
			ts, cluster.Namespace, externalLocal(cell.Spec.LocalTopoServer),
		)
	})
}

func cellReferencesTopoServer(ts *api.TopoServer, cell *api.Cell) bool {
	return topology.CellReferencesTopoServer(ts, cell) ||
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
		condition.Reason, condition.Message = "ManagementDisabled", "Managed topology authorization has not been requested"
		return true, nil
	}
	if ts.Spec.Etcd == nil || !ts.Spec.TLS.IsEnabled() {
		return false, fmt.Errorf("topology authorization requires managed etcd with TLS")
	}
	plan, err := r.planTopologyAuth(ctx, ts, ts.Status.AuthEnabled)
	if err != nil {
		return false, err
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
	passCtx, cancel := context.WithTimeout(ctx, authPassTimeout)
	defer cancel()
	var auth etcdAuthClient
	var status *clientv3.AuthStatusResponse
	if ts.Status.AuthEnabled {
		var closeClient func()
		auth, closeClient, err = r.openEtcdAuth(passCtx, ts)
		if err != nil {
			return false, err
		}
		defer closeClient()
		statusCtx, cancelStatus := context.WithTimeout(passCtx, authIdentityTimeout)
		status, err = readEtcdAuthStatus(statusCtx, auth)
		cancelStatus()
		if err != nil {
			return false, err
		}
		if !status.Enabled {
			plan, err = r.planTopologyAuth(passCtx, ts, false)
			if err != nil {
				return false, err
			}
		}
	}
	if err := r.reconcileEtcdIdentities(
		passCtx,
		ts,
		plan.desired,
		plan.frozen,
		auth,
		status,
	); err != nil {
		return false, errors.Join(err, plan.refusal)
	}
	if plan.refusal != nil {
		return false, plan.refusal
	}
	ts.Status.AuthEnabled = true
	condition.Status, condition.Reason, condition.Message = metav1.ConditionTrue, "Reconciled", "Topology authorization grants are reconciled"
	if len(plan.desired) == 0 {
		condition.Message = "Topology authorization is enabled and no cluster references this server yet"
	}
	return true, nil
}

type topologyAuthPlan struct {
	desired []etcdIdentityGrant
	frozen  []string
	refusal error
}

func (r *TopoServerReconciler) planTopologyAuth(
	ctx context.Context,
	ts *api.TopoServer,
	activated bool,
) (topologyAuthPlan, error) {
	discoveryReader := client.Reader(r.Client)
	if !activated {
		if r.APIReader == nil {
			return topologyAuthPlan{}, fmt.Errorf(
				"cannot activate topology authorization without an uncached API reader",
			)
		}
		discoveryReader = r.APIReader
	}
	discoveryTarget := *ts
	discoveryTarget.Status.AuthEnabled = activated
	desired, refused, err := desiredEtcdGrants(ctx, discoveryReader, &discoveryTarget)
	if err != nil {
		return topologyAuthPlan{}, err
	}
	desired, refused, err = r.confirmIdentityRemoval(ctx, ts, desired, refused)
	if err != nil {
		return topologyAuthPlan{}, fmt.Errorf("confirming topology identity removal: %w", err)
	}
	// A refusal prevents activation. An active server keeps its refused
	// identities unchanged while the other identities reconcile.
	var refusal error
	if len(refused) > 0 {
		refusal = &projectRefRequiredError{refused: refused}
		if !activated {
			return topologyAuthPlan{}, refusal
		}
	}
	frozen := frozenIdentities(ts, refused)
	desired = slices.DeleteFunc(desired, func(grant etcdIdentityGrant) bool {
		return slices.Contains(frozen, grant.Name)
	})
	if len(desired) == 0 && !activated && metav1.GetControllerOf(ts) != nil {
		return topologyAuthPlan{}, fmt.Errorf("no project identities resolved for topology server")
	}
	return topologyAuthPlan{desired: desired, frozen: frozen, refusal: refusal}, nil
}

func authFailureReason(err error) string {
	var failures *identityFailuresError
	var refused *projectRefRequiredError
	switch {
	case errors.As(err, &failures):
		return "IdentitiesFailed"
	case errors.As(err, &refused):
		return "ProjectRefRequired"
	default:
		return "ReconcileFailed"
	}
}
