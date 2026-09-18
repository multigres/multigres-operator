package suite

import (
	"fmt"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ctrltest"
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
	ns := Suite.Namespace(t)
	cluster := MinimalCluster(t, ns, "quiesce")

	ctrltest.Eventually(t, 30*time.Second, "cluster to report Healthy", func() error {
		got := &multigresv1alpha1.MultigresCluster{}
		if err := Suite.Client.Get(
			t.Context(),
			client.ObjectKeyFromObject(cluster),
			got,
		); err != nil {
			return err
		}
		if got.Status.Phase != multigresv1alpha1.PhaseHealthy {
			return fmt.Errorf("phase is %q", got.Status.Phase)
		}
		return nil
	})

	Suite.RequireQuiescent(t, ns, 10*time.Second, 30*time.Second)
}
