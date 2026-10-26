package multigrescluster

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/features"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

func TestReconcile_GatedBackstopWritesNothing(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	testutil.RequireGatedReconcileIsNoop(t, scheme, &multigresv1alpha1.MultigresCluster{},
		func(c client.Client, gate features.Gate) reconcile.Reconciler {
			return &MultigresClusterReconciler{
				Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10), Gate: gate,
			}
		})
}
