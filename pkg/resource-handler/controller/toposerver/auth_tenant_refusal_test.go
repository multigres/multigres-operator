package toposerver

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func requireConfigRefusal(
	t *testing.T,
	refused []clusterRefusal,
	cluster *api.MultigresCluster,
	contains string,
) {
	t.Helper()
	require.Len(t, refused, 1)
	require.Equal(t, client.ObjectKeyFromObject(cluster), refused[0].cluster)
	require.True(t, refused[0].invalidConfig)
	require.Contains(t, refused[0].reason, contains)
}

var tenantConfigErrors = map[string]struct {
	configure func(*api.MultigresCluster)
	cause     string
}{
	"custom credentials": {
		configure: func(cluster *api.MultigresCluster) {
			cluster.Spec.GlobalTopoServer.External.ClientCertSecret = "custom-client"
		},
		cause: "an operator-issued topology client certificate is required",
	},
	"unresolved Cell template": {
		configure: func(cluster *api.MultigresCluster) {
			cluster.Spec.Cells = []api.CellConfig{{Name: "zone", CellTemplate: "missing"}}
		},
		cause: "referenced CellTemplate 'missing' not found",
	},
}

func TestTopologyAuthRefusesTenantConfigErrorAfterActivation(t *testing.T) {
	for name, tc := range tenantConfigErrors {
		t.Run(name, func(t *testing.T) {
			r, ts, _, auth := authReconcileFixture(t)
			gone := authGrantCluster("gone", "gone")
			require.NoError(t, r.Create(t.Context(), gone))
			_, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.NoError(t, err)
			require.Contains(t, auth.users, "/multigres/gone")

			bad := authGrantCluster("bad", "bad")
			tc.configure(bad)
			require.NoError(t, r.Create(t.Context(), bad))
			require.NoError(t, r.Create(t.Context(), authGrantCluster("newcomer", "newcomer")))
			require.NoError(t, r.Delete(t.Context(), gone))

			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.False(t, ready)
			var refused *refusedClustersError
			require.ErrorAs(t, err, &refused)
			requireConfigRefusal(t, refused.refused, bad, tc.cause)
			requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ClusterConfigInvalid")
			requireAuthorizationMessage(t, ts, "projects/bad", tc.cause)

			require.Contains(t, auth.users, "/multigres/newcomer")
			require.NotContains(t, auth.users, "/multigres/gone")
			require.NotContains(t, auth.roles, "/multigres/gone")
			require.NotContains(t, auth.users, "/multigres/bad")
			require.Equal(
				t,
				[]string{"/multigres/newcomer", "/multigres/project"},
				ts.Status.AuthIdentities,
			)
		})
	}
}

func TestTopologyAuthFailsClosedOnTenantConfigErrorBeforeActivation(t *testing.T) {
	for name, tc := range tenantConfigErrors {
		t.Run(name, func(t *testing.T) {
			r, ts, _, auth := authReconcileFixture(t)
			bad := authGrantCluster("bad", "bad")
			tc.configure(bad)
			require.NoError(t, r.Create(t.Context(), bad))
			require.NoError(t, r.Create(t.Context(), authGrantCluster("newcomer", "newcomer")))

			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.ErrorContains(t, err, tc.cause)
			var refused *refusedClustersError
			require.False(t, errors.As(err, &refused))
			require.False(t, ready)
			require.False(t, ts.Status.AuthEnabled)
			require.Empty(t, auth.mutations)
			require.Empty(t, ts.Status.AuthIdentities)
			requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ReconcileFailed")
		})
	}
}

func TestTopologyAuthKeepsGrantsOfTenantWithConfigError(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	second := authGrantCluster("second", "second")
	require.NoError(t, r.Create(t.Context(), second))
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	const identity = "/multigres/second"
	permissions := rolePermissionKeys(auth, identity)
	require.NotEmpty(t, permissions)

	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(second), second))
	second.Spec.GlobalTopoServer.External.ClientCertSecret = "custom-client"
	require.NoError(t, r.Update(t.Context(), second))
	require.NoError(t, r.Create(t.Context(), authGrantCluster("newcomer", "newcomer")))

	_, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "projects/second")
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ClusterConfigInvalid")
	require.Equal(t, []string{identity}, auth.users[identity])
	require.Equal(t, permissions, rolePermissionKeys(auth, identity))
	require.Equal(t, "projects/second", ts.Status.AuthIdentityClusters[identity])
	require.Contains(t, auth.users, "/multigres/newcomer")
	require.Equal(
		t,
		[]string{"/multigres/newcomer", "/multigres/project", identity},
		ts.Status.AuthIdentities,
	)
}

func TestTopologyAuthRefusesDepartedTenantWithConfigError(t *testing.T) {
	r, ts, first, auth := authReconcileFixture(t)
	second := authGrantCluster("second", "second")
	require.NoError(t, r.Create(t.Context(), second))
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	const identity = "/multigres/second"

	require.NoError(t, r.Delete(t.Context(), second))
	second.Spec.Cells = []api.CellConfig{{Name: "zone", CellTemplate: "missing"}}
	r.APIReader = authConfirmationReader(t, r, first.DeepCopy(), second)
	auth.mutations = nil

	_, err = r.reconcileTopologyAuth(t.Context(), ts)
	var refused *refusedClustersError
	require.ErrorAs(t, err, &refused)
	requireConfigRefusal(t, refused.refused, second, "referenced CellTemplate 'missing' not found")
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ClusterConfigInvalid")
	require.Empty(t, auth.mutations)
	require.Contains(t, auth.users, identity)
	require.Equal(t, []string{"/multigres/project", identity}, ts.Status.AuthIdentities)
}

func TestTopologyAuthRetriesTransientTenantReadError(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)

	flaky := authGrantCluster("flaky", "flaky")
	flaky.Spec.GlobalTopoServer.TemplateRef = "flaky"
	require.NoError(t, r.Create(t.Context(), flaky))
	r.Client = testutil.NewFakeClientWithFailures(r.Client, &testutil.FailureConfig{
		OnGet: func(key client.ObjectKey) error {
			if key.Name == "flaky" {
				return errors.New("template read unavailable")
			}
			return nil
		},
	})
	auth.mutations = nil

	_, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "template read unavailable")
	var refused *refusedClustersError
	require.False(t, errors.As(err, &refused))
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ReconcileFailed")
	require.Empty(t, auth.mutations)
}

func TestAuthFailureReasonSeparatesConfigRefusals(t *testing.T) {
	noref := clusterRefusal{
		cluster: client.ObjectKey{Namespace: "tenants", Name: "noref"},
		reason:  "has no " + metadata.AnnotationProjectRef + " annotation",
	}
	custom := clusterRefusal{
		cluster:       client.ObjectKey{Namespace: "tenants", Name: "custom"},
		reason:        "has a topology configuration that cannot be authorized",
		invalidConfig: true,
	}
	require.Equal(t, "ProjectRefRequired",
		authFailureReason(&refusedClustersError{refused: []clusterRefusal{noref}}))
	require.Equal(t, "ClusterConfigInvalid",
		authFailureReason(&refusedClustersError{refused: []clusterRefusal{noref, custom}}))
}
