package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	coordinationv1 "k8s.io/api/coordination/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const leaderElectionID = "multigres-operator.multigres.com"

// deployedOperatorNamespaces returns every namespace holding a
// leaderElectionID lease, which is where a deployed operator runs. It searches
// all namespaces because the install namespace is the installer's choice, and
// POD_NAMESPACE cannot stand in for it: out of cluster it defaults to
// "default", where no deployed operator's lease is, so a lookup keyed on it or
// on any fixed name would fail open on every other install.
func deployedOperatorNamespaces(ctx context.Context, r client.Reader) ([]string, error) {
	var leases coordinationv1.LeaseList
	byName := client.MatchingFields{"metadata.name": leaderElectionID}
	if err := r.List(ctx, &leases, byName); err != nil {
		return nil, fmt.Errorf(
			"cannot list leases named %s to tell whether an operator is deployed here: %w",
			leaderElectionID, err,
		)
	}
	namespaces := make([]string, 0, len(leases.Items))
	for _, l := range leases.Items {
		namespaces = append(namespaces, l.Namespace)
	}
	slices.Sort(namespaces)
	return namespaces, nil
}

// deployedOperatorConflict reports whether a cluster-wide run from outside the
// cluster could reconcile alongside a deployed operator, given the namespaces
// deployedOperatorNamespaces found.
//
// Existence is the whole test, not liveness. A cluster where an operator has
// ever been deployed has the lease until its namespace is deleted, so a bare
// Kind cluster is the only case that clears. Judging liveness instead would
// pass during every rollout, when the old leader has released the lease
// (LeaderElectionReleaseOnCancel) and the new one has not yet acquired it, and
// a laptop started in that window would run cluster-wide for its lifetime.
func deployedOperatorConflict(namespaces []string) error {
	if len(namespaces) == 0 {
		return nil
	}
	return fmt.Errorf(
		"lease %s exists in namespace(s) %s, so an operator is or was deployed on this "+
			"cluster and a cluster-wide run could fight it over every object (use "+
			"--namespace with a namespace labelled multigres.com/reconcile=disabled, or "+
			"delete the lease if no operator is deployed)",
		leaderElectionID, strings.Join(namespaces, ", "),
	)
}
