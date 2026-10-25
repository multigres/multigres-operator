package shard

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
)

// withShardOwner stamps the Shard controller's field manager on c. The fake
// client rejects an Apply that names no manager, and a ShardReconciler literal
// bypasses SetupWithManagerReconciler, which is what stamps it in production.
// Tests that build their client with bespoke options (interceptors, a narrower
// scheme) wrap it here; the rest use newTestClient.
func withShardOwner(c client.Client) client.Client {
	return client.WithFieldOwner(c, ownership.ManagerShard)
}

// newTestClient returns a fake client holding objs that writes as the Shard
// controller. Status is a subresource for every kind the controller writes
// status on, as it is on a real apiserver, so a plain Update cannot smuggle a
// status change through.
func newTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))
	require.NoError(t, storagev1.AddToScheme(scheme))
	return withShardOwner(fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(
			&multigresv1alpha1.Shard{},
			&appsv1.StatefulSet{},
			&appsv1.Deployment{},
			&corev1.Pod{},
		).
		Build())
}
