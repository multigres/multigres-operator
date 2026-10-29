package suite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// TestShardServiceAccountConflictIsNamedWithoutTheLabel pins that the conflict
// check does not depend on the informer. The suite runs the production cache
// options, which hold only ServiceAccounts carrying the operator's managed-by
// label, and a platform account owned by another controller carries no such
// label. The conflict must still be named, because the lookup reads through
// the APIReader.
func TestShardServiceAccountConflictIsNamedWithoutTheLabel(t *testing.T) {
	c := newCase(t)
	const saName = "platform-shard-sa"
	c.NoError(c.Create(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: c.NS,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "platform-owner",
				UID:        "platform-owner-uid",
				Controller: ptr.To(true),
			}},
		},
	}), "create the conflicting ServiceAccount")

	cluster := c.newCluster("sa-conflict", func(s *MultigresClusterSpec) {
		s.ShardServiceAccount = &multigresv1alpha1.ShardServiceAccountSpec{Name: saName}
	})

	c.Eventually(30*time.Second, "the cluster to report the conflict", func() error {
		got := &MultigresCluster{}
		if err := c.Get(client.ObjectKeyFromObject(cluster), got); err != nil {
			return err
		}
		cond := meta.FindStatusCondition(
			got.Status.Conditions,
			multigresv1alpha1.ConditionShardServiceAccountReady,
		)
		if cond == nil {
			return fmt.Errorf("no ShardServiceAccountReady condition yet")
		}
		if cond.Reason != multigresv1alpha1.ReasonShardServiceAccountConflict ||
			!strings.Contains(cond.Message, `Deployment "platform-owner"`) {
			return fmt.Errorf("condition is %s: %s", cond.Reason, cond.Message)
		}
		return nil
	})
}
