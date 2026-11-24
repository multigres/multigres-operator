package toposerver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func requireAuthorizationMessage(t *testing.T, ts *api.TopoServer, contains ...string) {
	t.Helper()
	condition := meta.FindStatusCondition(ts.Status.Conditions, "AuthorizationReady")
	require.NotNil(t, condition)
	for _, want := range contains {
		require.Contains(t, condition.Message, want)
	}
}

func TestTopologyAuthRefusesClusterWithoutProjectRefBeforeActivation(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	require.NoError(t, r.Create(t.Context(), fallbackRootCluster("tenants", "noref")))
	clients := 0
	factory := r.MaintenanceClientFactory
	r.MaintenanceClientFactory = func(ctx context.Context, ts *api.TopoServer) (EtcdMaintenanceClient, error) {
		clients++
		return factory(ctx, ts)
	}

	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "tenants/noref")
	require.ErrorContains(t, err, metadata.AnnotationProjectRef)
	require.False(t, ready)
	require.False(t, ts.Status.AuthEnabled)
	require.Zero(t, clients, "nothing may be written to etcd")
	require.Empty(t, auth.mutations)
	require.Empty(t, ts.Status.AuthIdentities, "not even the cluster that has a ref is tracked")
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ProjectRefRequired")
	requireAuthorizationMessage(t, ts, "tenants/noref", metadata.AnnotationProjectRef)
}

func TestTopologyAuthActivatesOnceRefusedClusterGainsProjectRef(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	noref := fallbackRootCluster("tenants", "noref")
	require.NoError(t, r.Create(t.Context(), noref))
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "tenants/noref")
	require.Empty(t, auth.mutations)

	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(noref), noref))
	noref.Annotations = map[string]string{metadata.AnnotationProjectRef: "tenant"}
	require.NoError(t, r.Update(t.Context(), noref))

	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.True(t, ts.Status.AuthEnabled)
	require.Equal(t, []string{"/multigres/project", "/multigres/tenant"}, ts.Status.AuthIdentities)
	require.Contains(t, auth.users, "/multigres/tenant")
	requireAuthorizationCondition(t, ts, metav1.ConditionTrue, "Reconciled")
}

func TestTopologyAuthAfterActivationDoesNotGrantClusterWithoutProjectRef(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	auth.clusterID = 7
	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)

	noref := fallbackRootCluster("tenants", "noref")
	require.NoError(t, r.Create(t.Context(), noref))
	require.NoError(t, r.Create(t.Context(), authGrantCluster("bystander", "bystander")))
	auth.mutations = nil

	ready, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "tenants/noref")
	require.False(t, ready)
	require.True(t, ts.Status.AuthEnabled)
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ProjectRefRequired")
	requireAuthorizationMessage(t, ts, "tenants/noref", metadata.AnnotationProjectRef)

	require.Equal(t, []string{"/multigres/bystander"}, auth.users["/multigres/bystander"])
	require.Equal(
		t,
		[]string{"/multigres/bystander", "/multigres/project"},
		ts.Status.AuthIdentities,
	)
	require.Equal(t,
		map[string]string{
			"/multigres/bystander": "projects/bystander",
			"/multigres/project":   "projects/cluster",
		},
		ts.Status.AuthIdentityClusters,
	)
	for name := range auth.users {
		require.NotContains(t, name, "tenants", "refused cluster must have no etcd user")
	}
	for name := range auth.roles {
		require.NotContains(t, name, "tenants", "refused cluster must have no etcd role")
	}

	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(noref), noref))
	noref.Annotations = map[string]string{metadata.AnnotationProjectRef: "tenant"}
	require.NoError(t, r.Update(t.Context(), noref))
	ready, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.Contains(t, auth.users, "/multigres/tenant")
	require.Equal(
		t,
		[]string{"/multigres/bystander", "/multigres/project", "/multigres/tenant"},
		ts.Status.AuthIdentities,
	)
	requireAuthorizationCondition(t, ts, metav1.ConditionTrue, "Reconciled")
}

func TestTopologyAuthKeepsAccessOfClusterThatLosesItsProjectRef(t *testing.T) {
	r, ts, cluster, auth := authReconcileFixture(t)
	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	const identity = "/multigres/project"

	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
	cluster.Annotations = nil
	require.NoError(t, r.Update(t.Context(), cluster))
	require.NoError(t, r.Create(t.Context(), authGrantCluster("other", "other")))

	for range 3 {
		ready, err = r.reconcileTopologyAuth(t.Context(), ts)
		require.ErrorContains(t, err, "projects/cluster")
		require.False(t, ready)
		require.Equal(t, []string{identity}, auth.users[identity])
		require.Equal(t, []string{"/multigres/project/global/"}, rolePermissionKeys(auth, identity))
		require.Equal(t, []string{"/multigres/other", identity}, ts.Status.AuthIdentities)
		require.Equal(t, "projects/cluster", ts.Status.AuthIdentityClusters[identity])
		require.Contains(t, auth.users, "/multigres/other")
		requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ProjectRefRequired")
		requireAuthorizationMessage(t, ts,
			"keep the topology access they already have",
			"projects/cluster has no "+metadata.AnnotationProjectRef,
		)
	}

	require.NoError(t, r.Delete(t.Context(), cluster))
	ready, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.NotContains(t, auth.users, identity)
	require.NotContains(t, auth.roles, identity)
	require.Equal(t, []string{"/multigres/other"}, ts.Status.AuthIdentities)
}

func TestTopologyAuthRefusesRootOutsideProjectPrefixAfterActivation(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)

	stale := authGrantCluster("stale", "stale")
	stale.Spec.GlobalTopoServer.External.RootPath = "/multigres/projects/stale/global"
	require.NoError(t, r.Create(t.Context(), stale))
	require.NoError(t, r.Create(t.Context(), authGrantCluster("newcomer", "newcomer")))

	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(
		t,
		err,
		`projects/stale has topology root "/multigres/projects/stale/global"`,
	)
	require.False(t, ready)
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ProjectRefRequired")
	require.Contains(t, auth.users, "/multigres/newcomer")
	require.NotContains(t, auth.users, "/multigres/stale")
	require.Equal(
		t,
		[]string{"/multigres/newcomer", "/multigres/project"},
		ts.Status.AuthIdentities,
	)
}

func TestTopologyAuthRefusalIsReportedBesideOtherProblems(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)

	require.NoError(t, r.Create(t.Context(), fallbackRootCluster("tenants", "noref")))
	require.NoError(t, r.Create(t.Context(), authGrantCluster("bad", "bad")))
	auth.failRoles = map[string]bool{"/multigres/bad": true}

	_, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "etcd identities failed to reconcile")
	require.ErrorContains(t, err, "tenants/noref")
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "IdentitiesFailed")
}

func TestTopologyAuthSharedServerWithOneClusterWithAndOneWithoutProjectRef(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	require.NoError(t, r.Create(t.Context(), fallbackRootCluster("projects", "legacy")))

	grants, refused, err := desiredEtcdGrants(t.Context(), r.Client, ts)
	require.NoError(t, err)
	require.Equal(t, []clusterRefusal{{
		cluster: client.ObjectKey{Namespace: "projects", Name: "legacy"},
		reason:  "has no " + metadata.AnnotationProjectRef + " annotation",
	}}, refused)
	require.Len(t, grants, 1)
	require.Equal(t, "/multigres/project", grants[0].Name)

	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "projects/legacy")
	require.False(t, ready)
	require.Empty(t, auth.mutations)
}

func TestTopologyWithoutAuthKeepsFallbackRoots(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	ts.Spec.Auth = nil
	require.NoError(t, r.Update(t.Context(), ts))
	require.NoError(t, r.Create(t.Context(), fallbackRootCluster("tenants", "noref")))

	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.Empty(t, auth.mutations)
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ManagementDisabled")
}
