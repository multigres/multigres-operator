package toposerver

import (
	"slices"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// clusterRefusal is a referencing cluster that an auth-managed server grants
// nothing, because it has no project reference or a root outside its project
// prefix. A derived root can sit inside another cluster's prefix.
type clusterRefusal struct {
	cluster client.ObjectKey
	reason  string
}

type projectRefRequiredError struct {
	refused []clusterRefusal
}

func (e *projectRefRequiredError) Error() string {
	parts := make([]string, 0, len(e.refused))
	for _, refusal := range e.refused {
		parts = append(parts, refusal.cluster.String()+" "+refusal.reason)
	}
	return "refused clusters keep the topology access they already have, and get no new access, " +
		"until they are fixed or deleted: " + cappedList(parts, "; ")
}

func refusesCluster(refused []clusterRefusal, key client.ObjectKey) bool {
	return slices.ContainsFunc(refused, func(refusal clusterRefusal) bool {
		return refusal.cluster == key
	})
}
