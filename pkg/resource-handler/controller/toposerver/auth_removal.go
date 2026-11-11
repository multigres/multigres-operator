package toposerver

import (
	"context"
	"fmt"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
)

// confirmIdentityRemoval re-reads, uncached, the clusters that required the
// removed identities and returns desired plus any identity they still require.
// Identities recorded without a cluster fall back to uncached discovery of
// every cluster and Cell.
func (r *TopoServerReconciler) confirmIdentityRemoval(
	ctx context.Context,
	ts *api.TopoServer,
	desired []etcdIdentityGrant,
	removed []string,
) ([]etcdIdentityGrant, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("cannot confirm without an uncached API reader")
	}
	clusterKeys, recorded := removedIdentityClusters(ts, removed)
	if !recorded {
		return desiredEtcdGrantsWithDiscovery(ctx, r.APIReader, ts, false)
	}
	grants := map[string]*etcdIdentityGrant{}
	for _, key := range clusterKeys {
		cluster := &api.MultigresCluster{}
		if err := r.APIReader.Get(ctx, key, cluster); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("reading cluster %s: %w", key, err)
		}
		cells := &api.CellList{}
		if err := listClusterCells(ctx, r.APIReader, cluster, cells); err != nil {
			return nil, err
		}
		clusterCells := indexCellsByCluster(cells.Items)[key]
		if err := addClusterGrants(
			ctx,
			r.APIReader,
			ts,
			cluster,
			clusterCells,
			grants,
		); err != nil {
			return nil, err
		}
	}
	confirmed := slices.Clone(desired)
	for _, grant := range finalizeGrants(grants) {
		if slices.Contains(removed, grant.Name) {
			confirmed = append(confirmed, grant)
		}
	}
	slices.SortFunc(confirmed, func(a, b etcdIdentityGrant) int {
		return strings.Compare(a.Name, b.Name)
	})
	return confirmed, nil
}

func removedIdentityClusters(ts *api.TopoServer, removed []string) ([]client.ObjectKey, bool) {
	var keys []client.ObjectKey
	for _, identity := range removed {
		recorded := ts.Status.AuthIdentityClusters[identity]
		if recorded == "" {
			return nil, false
		}
		for _, entry := range strings.Split(recorded, ",") {
			namespace, name, ok := strings.Cut(entry, "/")
			if !ok || namespace == "" || name == "" {
				return nil, false
			}
			key := client.ObjectKey{Namespace: namespace, Name: name}
			if !slices.Contains(keys, key) {
				keys = append(keys, key)
			}
		}
	}
	return keys, true
}
