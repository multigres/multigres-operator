//go:build e2e

package postgresconfig_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/testkit/assert"
)

// poolPodUIDs returns the current pool pods (component shard-pool) in ns keyed by
// name -> UID. Tests snapshot it before and after a config change to tell a reload
// (stable UIDs) apart from a restart (recreated pods, new UIDs).
func poolPodUIDs(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	ns string,
) map[string]types.UID {
	t.Helper()
	pods := &corev1.PodList{}
	assert.NewAborting(t).NoError(c.List(ctx, pods, client.InNamespace(ns),
		client.MatchingLabels{"app.kubernetes.io/component": "shard-pool"}), "list pool pods")
	uids := map[string]types.UID{}
	for i := range pods.Items {
		uids[pods.Items[i].Name] = pods.Items[i].UID
	}
	return uids
}
