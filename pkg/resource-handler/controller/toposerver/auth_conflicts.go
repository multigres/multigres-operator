package toposerver

import (
	"fmt"
	"slices"
	"strings"
)

// grantConflict is a pair of identities whose permissions reach the same keys.
type grantConflict struct {
	// clusters name the clusters behind each identity.
	clusters [2]string
	// identities are the etcd identities that carry the overlapping ranges.
	identities [2]string
}

func (c grantConflict) String() string {
	return fmt.Sprintf(
		"cluster %s (%s) and cluster %s (%s)",
		c.clusters[0], c.identities[0], c.clusters[1], c.identities[1],
	)
}

// grantConflicts finds identities whose grants overlap. Project-ref roots and
// fallback roots share one key space, so a fallback root can sit inside another
// project's prefix and one identity would reach the other's records. Clusters
// that share one identity are the same project and do not conflict.
func grantConflicts(grants []etcdIdentityGrant) []grantConflict {
	var conflicts []grantConflict
	type span struct {
		start, end string
		grant      int
	}
	var spans []span
	for i, grant := range grants {
		for _, permission := range grant.Permissions {
			spans = append(spans, span{permission.Key, permission.RangeEnd, i})
		}
	}
	slices.SortFunc(spans, func(a, b span) int { return strings.Compare(a.start, b.start) })
	reported := map[[2]int]bool{}
	var active []span
	for _, current := range spans {
		active = slices.DeleteFunc(active, func(s span) bool { return s.end <= current.start })
		for _, other := range active {
			if other.grant == current.grant {
				continue
			}
			pair := [2]int{min(current.grant, other.grant), max(current.grant, other.grant)}
			if reported[pair] {
				continue
			}
			reported[pair] = true
			a, b := grants[pair[0]], grants[pair[1]]
			conflicts = append(conflicts, grantConflict{
				clusters:   [2]string{a.clusterNames(), b.clusterNames()},
				identities: [2]string{a.Name, b.Name},
			})
		}
		active = append(active, current)
	}
	slices.SortFunc(conflicts, func(a, b grantConflict) int {
		return strings.Compare(a.String(), b.String())
	})
	return conflicts
}

func conflictedIdentities(conflicts []grantConflict) map[string]bool {
	names := map[string]bool{}
	for _, conflict := range conflicts {
		names[conflict.identities[0]] = true
		names[conflict.identities[1]] = true
	}
	return names
}

// grantConflictError reports overlapping grants.
type grantConflictError struct {
	conflicts []grantConflict
}

func (e *grantConflictError) Error() string {
	shown := e.conflicts
	if len(shown) > maxReportedFailures {
		shown = shown[:maxReportedFailures]
	}
	parts := make([]string, 0, len(shown))
	for _, conflict := range shown {
		parts = append(parts, conflict.String())
	}
	more := ""
	if len(e.conflicts) > len(shown) {
		more = fmt.Sprintf(" (and %d more)", len(e.conflicts)-len(shown))
	}
	return "topology roots overlap: " + strings.Join(parts, "; ") + more
}
