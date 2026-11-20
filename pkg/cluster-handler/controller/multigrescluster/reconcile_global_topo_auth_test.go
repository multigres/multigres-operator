package multigrescluster

import (
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestResolvedTopoAuthProblem(t *testing.T) {
	authOn := &multigresv1alpha1.TopoServerAuthSpec{Enabled: true}
	authOff := &multigresv1alpha1.TopoServerAuthSpec{Enabled: false}
	managed := func(
		auth *multigresv1alpha1.TopoServerAuthSpec,
	) *multigresv1alpha1.GlobalTopoServerSpec {
		return &multigresv1alpha1.GlobalTopoServerSpec{
			Auth: auth,
			Etcd: &multigresv1alpha1.EtcdSpec{},
		}
	}
	external := &multigresv1alpha1.GlobalTopoServerSpec{
		External: &multigresv1alpha1.ExternalTopoServerSpec{
			Endpoints: []multigresv1alpha1.EndpointUrl{"https://external-etcd:2379"},
		},
	}
	childRequestedAuth := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-global-topo"},
		Spec:       multigresv1alpha1.TopoServerSpec{Auth: authOn},
	}
	childActivatedAuth := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-global-topo"},
		Status:     multigresv1alpha1.TopoServerStatus{AuthEnabled: true},
	}
	childWithoutAuth := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-global-topo"},
	}
	withTLS := topoTLSCluster("cluster", "default", nil)
	withoutTLS := topoTLSCluster("cluster", "default", nil)
	withoutTLS.Spec.TopoTLS = nil

	tests := map[string]struct {
		cluster      *multigresv1alpha1.MultigresCluster
		resolved     *multigresv1alpha1.GlobalTopoServerSpec
		existing     *multigresv1alpha1.TopoServer
		wantReason   string
		wantInAdvice []string
	}{
		"auth with topology TLS": {
			cluster:  withTLS,
			resolved: managed(authOn),
		},
		"auth with topology TLS on a child that already has it": {
			cluster:  withTLS,
			resolved: managed(authOn),
			existing: childActivatedAuth,
		},
		"auth without topology TLS": {
			cluster:    withoutTLS,
			resolved:   managed(authOn),
			wantReason: reasonTopologyAuthRequiresTLS,
			wantInAdvice: []string{
				"spec.globalTopoServer.auth", "spec.topoTLS", "CoreTemplate",
			},
		},
		"auth dropped after the child requested it": {
			cluster:    withTLS,
			resolved:   managed(nil),
			existing:   childRequestedAuth,
			wantReason: reasonTopologyAuthCannotBeDisabled,
			wantInAdvice: []string{
				"spec.globalTopoServer.auth", "CoreTemplate", "cluster-global-topo",
			},
		},
		"auth set to false after the child activated it": {
			cluster:    withTLS,
			resolved:   managed(authOff),
			existing:   childActivatedAuth,
			wantReason: reasonTopologyAuthCannotBeDisabled,
		},
		"move to external from a child with auth": {
			cluster:  withTLS,
			resolved: external,
			existing: childRequestedAuth,
		},
		"no child yet": {
			cluster:  withTLS,
			resolved: managed(nil),
		},
		"auth never requested": {
			cluster:  withoutTLS,
			resolved: managed(nil),
			existing: childWithoutAuth,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			reason, message := resolvedTopoAuthProblem(tc.cluster, tc.resolved, tc.existing)
			assert.Equal(t, tc.wantReason, reason)
			if tc.wantReason == "" {
				assert.Empty(t, message)
				return
			}
			for _, want := range tc.wantInAdvice {
				assert.Contains(t, message, want)
			}
		})
	}
}

func TestReconcileTemplateAuthWithoutTopoTLS(t *testing.T) {
	cluster := topoTLSCluster("cluster", "default", nil)
	cluster.Spec.TopoTLS = nil
	cluster.Generation = 3
	template := &multigresv1alpha1.CoreTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default"},
		Spec: multigresv1alpha1.CoreTemplateSpec{
			GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
				Auth: &multigresv1alpha1.TopoServerAuthSpec{Enabled: true},
				Etcd: &multigresv1alpha1.EtcdSpec{},
			},
		},
	}
	scheme := setupScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, template).
		WithStatusSubresource(cluster).Build()
	recorder := record.NewFakeRecorder(100)
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
		CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return noCloseStore{Store: newClusterTopologyMemoryStore(t)}, nil
		},
	}
	key := client.ObjectKeyFromObject(cluster)
	childKey := client.ObjectKey{Name: "cluster-global-topo", Namespace: "default"}

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.ErrorContains(t, err, "spec.topoTLS")

	require.NoError(t, c.Get(t.Context(), key, cluster))
	assert.Equal(t, multigresv1alpha1.PhaseDegraded, cluster.Status.Phase)
	condition := meta.FindStatusCondition(cluster.Status.Conditions, conditionTopologyReady)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionFalse, condition.Status)
	assert.Equal(t, reasonTopologyAuthRequiresTLS, condition.Reason)
	assert.Equal(t, cluster.Generation, condition.ObservedGeneration)
	assert.Contains(t, condition.Message, "CoreTemplate")

	assert.Contains(t, drainEvents(recorder), "Warning "+reasonTopologyAuthRequiresTLS)

	err = c.Get(t.Context(), childKey, &multigresv1alpha1.TopoServer{})
	assert.True(t, apierrors.IsNotFound(err), "child TopoServer must not be applied, got %v", err)

	template.Spec.GlobalTopoServer.Auth = nil
	require.NoError(t, c.Update(t.Context(), template))
	_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)

	require.NoError(t, c.Get(t.Context(), key, cluster))
	condition = meta.FindStatusCondition(cluster.Status.Conditions, conditionTopologyReady)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.NotContains(t, cluster.Status.Message, "spec.topoTLS")
	require.NoError(t, c.Get(t.Context(), childKey, &multigresv1alpha1.TopoServer{}))
}

func drainEvents(recorder *record.FakeRecorder) string {
	var events string
	for {
		select {
		case event := <-recorder.Events:
			events += event + "\n"
		default:
			return events
		}
	}
}
