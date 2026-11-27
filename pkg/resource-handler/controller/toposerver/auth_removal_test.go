package toposerver

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func rolePermissionKeys(auth *fakeEtcdAuth, role string) []string {
	var keys []string
	for _, permission := range auth.roles[role] {
		keys = append(keys, string(permission.Key))
	}
	return keys
}

func TestTopologyAuthKeepsRangeOfClusterTheCacheMisses(t *testing.T) {
	ctx := t.Context()
	r, ts, cluster, auth := authReconcileFixture(t)
	second := authGrantCluster("project", "second")
	second.Spec.GlobalTopoServer.External.RootPath = "/multigres/project/second"
	require.NoError(t, r.Create(ctx, second))
	_, err := r.reconcileTopologyAuth(ctx, ts)
	require.NoError(t, err)
	const identity = "/multigres/project"
	both := []string{"/multigres/project/global/", "/multigres/project/second/"}
	require.ElementsMatch(t, both, rolePermissionKeys(auth, identity))

	r.APIReader = authConfirmationReader(t, r, cluster.DeepCopy(), second.DeepCopy())
	require.NoError(t, r.Delete(ctx, second))
	ready, err := r.reconcileTopologyAuth(ctx, ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.ElementsMatch(t, both, rolePermissionKeys(auth, identity))
	require.Equal(t, "projects/cluster,projects/second", ts.Status.AuthIdentityClusters[identity])
}

func TestTopologyAuthConfirmsRangeRemovalForRemainingCluster(t *testing.T) {
	for _, scenario := range []string{
		"previously verified", "after restart", "owner without label", "owner with stale label",
	} {
		t.Run(scenario, func(t *testing.T) {
			ctx := t.Context()
			r, ts, cluster, auth := authReconcileFixture(t)
			r.authVerified = &authVerifyCache{}
			auth.clusterID = 7
			cell := &api.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name: "zone", Namespace: cluster.Namespace,
					Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
				},
				Spec: api.CellSpec{
					Name: "zone",
					TopoServer: &api.LocalTopoServerSpec{External: &api.ExternalTopoServerSpec{
						Endpoints: []api.EndpointUrl{"https://shared.stores:2379"},
						RootPath:  "/multigres/project/zone",
					}},
				},
			}
			switch scenario {
			case "owner without label":
				cell.OwnerReferences = controllerRef("MultigresCluster", cluster)
				cell.Labels = nil
			case "owner with stale label":
				cell.OwnerReferences = controllerRef("MultigresCluster", cluster)
				cell.Labels[metadata.LabelMultigresCluster] = "different-cluster"
			}
			require.NoError(t, r.Create(ctx, cell))
			ready, err := r.reconcileTopologyAuth(ctx, ts)
			require.NoError(t, err)
			require.True(t, ready)
			const identity = "/multigres/project"
			const global = "/multigres/project/global/"
			require.ElementsMatch(
				t,
				[]string{global, "/multigres/project/zone/"},
				rolePermissionKeys(auth, identity),
			)

			authoritative := authConfirmationReader(t, r, cluster.DeepCopy(), cell.DeepCopy())
			r.APIReader = authoritative
			if scenario == "after restart" {
				r.authVerified = &authVerifyCache{}
			}
			require.NoError(t, r.Delete(ctx, cell))
			auth.mutations = nil
			ready, err = r.reconcileTopologyAuth(ctx, ts)
			require.ErrorContains(t, err, "cannot confirm range removal")
			require.False(t, ready)
			require.Empty(t, auth.mutations)
			require.ElementsMatch(
				t,
				[]string{global, "/multigres/project/zone/"},
				rolePermissionKeys(auth, identity),
			)
			require.Equal(t, "projects/cluster", ts.Status.AuthIdentityClusters[identity])

			require.NoError(t, authoritative.Delete(ctx, cell))
			ready, err = r.reconcileTopologyAuth(ctx, ts)
			require.NoError(t, err)
			require.True(t, ready)
			require.Equal(t, []string{global}, rolePermissionKeys(auth, identity))
		})
	}
}

func TestTopologyAuthDoesNotBackfillIncompleteClusterRecord(t *testing.T) {
	ctx := t.Context()
	r, ts, cluster, auth := authReconcileFixture(t)
	second := authGrantCluster("project", "second")
	second.Spec.GlobalTopoServer.External.RootPath = "/multigres/project/second"
	require.NoError(t, r.Create(ctx, second))
	_, err := r.reconcileTopologyAuth(ctx, ts)
	require.NoError(t, err)
	ts.Status.AuthIdentityClusters = nil
	require.NoError(t, r.Status().Update(ctx, ts))

	r.APIReader = authConfirmationReader(t, r, cluster.DeepCopy(), second.DeepCopy())
	require.NoError(t, r.Delete(ctx, second))
	const identity = "/multigres/project"
	both := []string{"/multigres/project/global/", "/multigres/project/second/"}
	for range 2 {
		ready, err := r.reconcileTopologyAuth(ctx, ts)
		require.ErrorContains(t, err, "identity has no recorded cluster")
		require.False(t, ready)
		require.ElementsMatch(t, both, rolePermissionKeys(auth, identity))
		require.NotContains(t, ts.Status.AuthIdentityClusters, identity)
	}

	second.ResourceVersion = ""
	require.NoError(t, r.Create(ctx, second))
	ready, err := r.reconcileTopologyAuth(ctx, ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, "projects/cluster,projects/second", ts.Status.AuthIdentityClusters[identity])
}

func TestTopologyAuthOwnerMovedToCustomCredentials(t *testing.T) {
	ctx := t.Context()
	r, ts, tenant, auth := authReconcileFixture(t)
	owner := authGrantCluster("owner", "owner")
	owner.Namespace = ts.Namespace
	owner.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{Etcd: &api.EtcdSpec{
		RootPath: "/multigres/owner/global",
	}}
	require.NoError(t, r.Create(ctx, owner))
	ts.OwnerReferences = controllerRef("MultigresCluster", owner)
	ts.Spec.Etcd.RootPath = "/multigres/owner/global"
	require.NoError(t, r.Update(ctx, ts))
	withCell := func(cluster *api.MultigresCluster) {
		require.NoError(t, r.Create(ctx, &api.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name: cluster.Name + "-zone", Namespace: cluster.Namespace,
				Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
			},
			Spec: api.CellSpec{Name: "zone", GlobalTopoServer: api.GlobalTopoServerRef{
				Address:  "https://shared.stores:2379",
				RootPath: "/multigres/" + cluster.Annotations[metadata.AnnotationProjectRef] + "/global",
			}},
		}))
	}
	departed := authGrantCluster("departed", "departed")
	require.NoError(t, r.Create(ctx, departed))
	withCell(tenant)
	withCell(departed)
	_, err := r.reconcileTopologyAuth(ctx, ts)
	require.NoError(t, err)
	require.Contains(t, auth.users, "/multigres/departed")

	owner.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{External: &api.ExternalTopoServerSpec{
		Endpoints:        []api.EndpointUrl{"https://other.stores:2379"},
		CASecret:         "custom-ca",
		ClientCertSecret: "custom-client",
	}}
	require.NoError(t, r.Update(ctx, owner))
	require.NoError(t, r.Delete(ctx, departed))
	require.NoError(t, r.Delete(ctx, &api.Cell{ObjectMeta: metav1.ObjectMeta{
		Name: "departed-zone", Namespace: departed.Namespace,
	}}))
	newcomer := authGrantCluster("newcomer", "newcomer")
	require.NoError(t, r.Create(ctx, newcomer))
	withCell(newcomer)

	ready, err := r.reconcileTopologyAuth(ctx, ts)
	require.NoError(t, err)
	require.True(t, ready)
	requireAuthorizationCondition(t, ts, metav1.ConditionTrue, "Reconciled")
	identities := []string{"/multigres/newcomer", "/multigres/owner", "/multigres/project"}
	require.Equal(t, identities, ts.Status.AuthIdentities)
	require.NotContains(t, auth.users, "/multigres/departed")
	require.Equal(
		t,
		[]string{"/multigres/owner/global/"},
		rolePermissionKeys(auth, "/multigres/owner"),
	)

	r.APIReader = authConfirmationReader(
		t, r, owner.DeepCopy(), tenant.DeepCopy(), newcomer.DeepCopy(),
	)
	require.NoError(t, r.Delete(ctx, owner))
	ready, err = r.reconcileTopologyAuth(ctx, ts)
	require.NoError(t, err, "the uncached re-read keeps the owner root too")
	require.True(t, ready)
	require.Equal(t, identities, ts.Status.AuthIdentities)
}

func TestDesiredEtcdGrantsKeepsOwnerRootAfterMove(t *testing.T) {
	owner := authGrantCluster("owner", "owner")
	owner.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{"https://other.stores:2379"}
	ts := &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "owner-global-topo", Namespace: owner.Namespace,
			OwnerReferences: controllerRef("MultigresCluster", owner),
		},
		Spec: api.TopoServerSpec{Etcd: &api.EtcdSpec{RootPath: "/multigres/owner/global"}},
	}

	grants, _, err := desiredEtcdGrants(t.Context(), authGrantReconciler(t, owner).Client, ts)
	require.NoError(t, err)
	require.Equal(t, []etcdIdentityGrant{{
		Name:        "/multigres/owner",
		Clusters:    []client.ObjectKey{client.ObjectKeyFromObject(owner)},
		Permissions: []etcdPermission{etcdPrefixPermission("/multigres/owner/global/")},
	}}, grants)
}

func TestTopologyAuthUsesDefaultCellTemplate(t *testing.T) {
	cluster := authGrantCluster("project", "cluster")
	cluster.Spec.TemplateDefaults.CellTemplate = "cells"
	cluster.Spec.Cells = []api.CellConfig{{Name: "zone"}}
	template := &api.CellTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "cells", Namespace: cluster.Namespace},
		Spec: api.CellTemplateSpec{LocalTopoServer: &api.LocalTopoServerSpec{
			External: &api.ExternalTopoServerSpec{
				Endpoints: []api.EndpointUrl{"https://shared.stores:2379"},
			},
		}},
	}
	ts := &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
		Spec:       api.TopoServerSpec{Auth: &api.TopoServerAuthSpec{Enabled: true}},
	}
	r := authGrantReconciler(t, cluster, template, ts)

	grants, _, err := desiredEtcdGrants(t.Context(), r.Client, ts)
	require.NoError(t, err)
	require.Len(t, grants, 1)
	require.Equal(t, []etcdPermission{
		etcdPrefixPermission("/multigres/project/global/"),
		etcdPrefixPermission("/multigres/project/zone/"),
	}, grants[0].Permissions)

	cluster.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{
		"https://other.stores:2379",
	}
	require.Equal(
		t,
		[]reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(ts)}},
		r.authManagedTopoServersForCluster(t.Context(), cluster),
	)
}
