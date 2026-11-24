//go:build integration
// +build integration

package toposerver_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/resource-handler/controller/toposerver"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// TestSpreadSelectorChangesReachTheStatefulSet applies the etcd StatefulSet
// through the no-op skip while a topology spread constraint's label selector
// moves between a populated selector, {} and no selector. A label selector is
// atomic, and {} selects every pod while no selector selects none, so each
// step changes scheduling and must be sent.
func TestSpreadSelectorChangesReachTheStatefulSet(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	cfg := testutil.SetUpEnvtest(t)
	c := ownership.SkipNoopApplies(testutil.SetUpClient(t, cfg, scheme))
	ctx := t.Context()

	populated := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "etcd"}}
	steps := []struct {
		name     string
		selector *metav1.LabelSelector
	}{
		{"populated", populated},
		{"cleared to {}", &metav1.LabelSelector{}},
		{"removed", nil},
		{"set to {}", &metav1.LabelSelector{}},
		{"populated again", populated},
	}
	for i, step := range steps {
		ts := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "spread",
				Namespace: "default",
				UID:       "spread-uid",
				Labels:    map[string]string{"multigres.com/cluster": "spread-cluster"},
			},
			Spec: multigresv1alpha1.TopoServerSpec{
				Placement: &multigresv1alpha1.TopoServerPlacementSpec{
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
						MaxSkew:           1,
						TopologyKey:       corev1.LabelTopologyZone,
						WhenUnsatisfiable: corev1.DoNotSchedule,
						LabelSelector:     step.selector,
					}},
				},
			},
		}
		sts, err := toposerver.BuildStatefulSet(ts, scheme)
		require.NoError(t, err)
		sts.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("StatefulSet"))
		changed, err := ownership.Apply(ctx, c, sts)
		require.NoError(t, err, step.name)
		require.True(t, changed, "%s: the apply changed the selector", step.name)

		got := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sts), got))
		stored := got.Spec.Template.Spec.TopologySpreadConstraints[0].LabelSelector
		require.Equal(t, step.selector, stored, "%s: stored selector", step.name)

		if i == len(steps)-1 {
			again, err := toposerver.BuildStatefulSet(ts, scheme)
			require.NoError(t, err)
			again.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("StatefulSet"))
			changed, err := ownership.Apply(ctx, c, again)
			require.NoError(t, err)
			require.False(t, changed, "a repeat apply changes nothing")
		}
	}
}
