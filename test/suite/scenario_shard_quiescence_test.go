package suite

import (
	"testing"
	"time"
)

// TestShardStatusQuiesces pins the fix for the shard status hot loop. It was
// written red, against the defect described below, and went green when the two
// server-side-apply defects behind it were fixed.
//
// A healthy Shard should stop writing once its status reflects reality, and it
// did not: two server-side-apply defects fought each other forever, so
// status.orchReady and status.poolsReady flipped false/true and the
// StorageClassValid condition's message alternated between two strings, each
// several times a second, with no terminal state. Keeping the measurement here
// is the point of the test: a status that converges is the property, and these
// are the fields that used to prove it did not.
func TestShardStatusQuiesces(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("quiesce")

	c.WaitForClusterHealthy(cluster)

	c.RequireQuiescent(10*time.Second, 30*time.Second)
}
