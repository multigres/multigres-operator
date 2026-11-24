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

// confirmIdentityRemoval re-reads, uncached, each cluster recorded for a
// tracked identity whose desired grant no longer lists it, so a stale cache
// cannot revoke a live cluster's grants. What those clusters still require is
// merged into desired, and the ones now refused are added to refused. Clusters
// already refused are not re-read. A tracked identity with no recorded cluster
// is an error, so nothing is pruned on a guess.
func (r *TopoServerReconciler) confirmIdentityRemoval(
	ctx context.Context,
	ts *api.TopoServer,
	desired []etcdIdentityGrant,
	refused []clusterRefusal,
) ([]etcdIdentityGrant, []clusterRefusal, error) {
	departed, err := departedClusters(ts, desired)
	if err != nil {
		return nil, nil, err
	}
	departed = slices.DeleteFunc(departed, func(key client.ObjectKey) bool {
		return refusesCluster(refused, key)
	})
	if len(departed) == 0 {
		return desired, refused, nil
	}
	if r.APIReader == nil {
		return nil, nil, fmt.Errorf("cannot confirm without an uncached API reader")
	}
	grants := make(map[string]*etcdIdentityGrant, len(desired))
	for _, grant := range desired {
		grant.Permissions = slices.Clone(grant.Permissions)
		grant.Clusters = slices.Clone(grant.Clusters)
		grants[grant.Name] = &grant
	}
	for _, key := range departed {
		reason, err := r.addLiveClusterGrants(ctx, ts, key, grants)
		if err != nil {
			return nil, nil, err
		}
		if reason != "" {
			refused = append(refused, clusterRefusal{cluster: key, reason: reason})
		}
	}
	return finalizeGrants(grants), refused, nil
}

func (r *TopoServerReconciler) confirmRangeRemoval(
	ctx context.Context,
	ts *api.TopoServer,
	desired etcdIdentityGrant,
	recorded string,
) error {
	if r.APIReader == nil {
		return fmt.Errorf("cannot confirm range removal without an uncached API reader")
	}
	keys, err := recordedClusterKeys(desired.Name, recorded)
	if err != nil {
		return err
	}
	for _, key := range desired.Clusters {
		if !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	grants := map[string]*etcdIdentityGrant{}
	for _, key := range keys {
		refusal, err := r.addLiveClusterGrants(ctx, ts, key, grants)
		if err != nil {
			return fmt.Errorf("confirming range removal for %q: %w", desired.Name, err)
		}
		if refusal != "" {
			return fmt.Errorf(
				"cannot confirm range removal for %q: cluster %s %s",
				desired.Name,
				key,
				refusal,
			)
		}
	}
	live := finalizeGrants(grants)
	i := slices.IndexFunc(live, func(grant etcdIdentityGrant) bool {
		return grant.Name == desired.Name
	})
	if i < 0 || !slices.Equal(live[i].Permissions, desired.Permissions) ||
		!slices.Equal(live[i].Clusters, desired.Clusters) {
		return fmt.Errorf(
			"cannot confirm range removal for %q: topology references changed",
			desired.Name,
		)
	}
	return nil
}

func (r *TopoServerReconciler) addLiveClusterGrants(
	ctx context.Context,
	ts *api.TopoServer,
	key client.ObjectKey,
	grants map[string]*etcdIdentityGrant,
) (string, error) {
	cluster := &api.MultigresCluster{}
	if err := r.APIReader.Get(ctx, key, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("reading cluster %s: %w", key, err)
	}
	cells := &api.CellList{}
	if err := r.APIReader.List(ctx, cells, client.InNamespace(key.Namespace)); err != nil {
		return "", fmt.Errorf("listing Cells for cluster %s: %w", key, err)
	}
	return addClusterGrants(
		ctx,
		r.APIReader,
		ts,
		cluster,
		indexCellsByCluster(cells.Items)[key],
		grants,
	)
}

// frozenIdentities returns the tracked identities recorded for a refused
// cluster. They stay as they are until the cluster is accepted or deleted.
func frozenIdentities(ts *api.TopoServer, refused []clusterRefusal) []string {
	var frozen []string
	for _, identity := range ts.Status.AuthIdentities {
		for _, entry := range strings.Split(ts.Status.AuthIdentityClusters[identity], ",") {
			namespace, name, _ := strings.Cut(entry, "/")
			if refusesCluster(refused, client.ObjectKey{Namespace: namespace, Name: name}) {
				frozen = append(frozen, identity)
				break
			}
		}
	}
	return frozen
}

// departedClusters returns the clusters recorded for tracked identities that
// their desired grants no longer list.
func departedClusters(
	ts *api.TopoServer,
	desired []etcdIdentityGrant,
) ([]client.ObjectKey, error) {
	var departed []client.ObjectKey
	for _, identity := range ts.Status.AuthIdentities {
		i := slices.IndexFunc(desired, func(grant etcdIdentityGrant) bool {
			return grant.Name == identity
		})
		recorded := ts.Status.AuthIdentityClusters[identity]
		if recorded == "" {
			if i < 0 {
				return nil, fmt.Errorf("identity %q has no recorded cluster", identity)
			}
			continue
		}
		keys, err := recordedClusterKeys(identity, recorded)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if (i < 0 || !slices.Contains(desired[i].Clusters, key)) &&
				!slices.Contains(departed, key) {
				departed = append(departed, key)
			}
		}
	}
	return departed, nil
}

func recordedClusterKeys(identity, recorded string) ([]client.ObjectKey, error) {
	var keys []client.ObjectKey
	if recorded == "" {
		return keys, nil
	}
	for _, entry := range strings.Split(recorded, ",") {
		namespace, name, ok := strings.Cut(entry, "/")
		if !ok || namespace == "" || name == "" {
			return nil, fmt.Errorf(
				"identity %q has an invalid recorded cluster %q",
				identity,
				entry,
			)
		}
		keys = append(keys, client.ObjectKey{Namespace: namespace, Name: name})
	}
	return keys, nil
}
