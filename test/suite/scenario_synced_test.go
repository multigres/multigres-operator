package suite

import (
	"strings"
	"testing"
	"time"
)

// TestSyncedOncePerGeneration requires one Synced event per object for each
// generation it converges on: when a cluster is created, and when a spec
// change rolls its pods. Neither converges on the pass that first observes the
// generation; both take passes that stop early and requeue while pods start
// or roll, so Synced must come from the pass that completes, not the one that
// observed the change.
func TestSyncedOncePerGeneration(t *testing.T) {
	c := newCase(t)

	stop := c.captureEvents()
	// The smallest pool whose pods the sim lets the operator replace.
	cluster := c.unreadyRollCluster("synced")
	c.RequireQuiescent(5*time.Second, 60*time.Second)
	created := synced(stop())
	for _, kind := range []string{"MultigresCluster", "TableGroup", "Shard", "TopoServer"} {
		c.True(created.ofKind(kind) == 1, "Synced for each %s of a new cluster: %v", kind, created)
	}
	created.requireAtMostOnce(c)

	stop = c.captureEvents()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
	c.RequireQuiescent(5*time.Second, 60*time.Second)
	rolled := synced(stop())
	c.True(rolled.ofKind("Shard") == 1, "Synced for the rolled Shard: %v", rolled)
	c.True(rolled.ofKind("MultigresCluster") == 1, "Synced for the edited cluster: %v", rolled)
	rolled.requireAtMostOnce(c)
}

// syncedEvents counts Synced events per object, as "Kind/name".
type syncedEvents map[string]int

// synced counts the Synced events among captured event lines, which read
// "controller *pkg.Kind name reason: message".
func synced(lines []string) syncedEvents {
	out := syncedEvents{}
	for _, line := range lines {
		f := strings.Fields(line)
		if len(f) < 4 || f[3] != "Synced:" {
			continue
		}
		kind := f[1][strings.LastIndex(f[1], ".")+1:]
		out[kind+"/"+f[2]]++
	}
	return out
}

func (s syncedEvents) ofKind(kind string) int {
	n := 0
	for obj, count := range s {
		if strings.HasPrefix(obj, kind+"/") {
			n += count
		}
	}
	return n
}

func (s syncedEvents) requireAtMostOnce(c *C) {
	c.Helper()
	for obj, count := range s {
		c.True(count <= 1, "%s recorded Synced %d times for one generation", obj, count)
	}
}
