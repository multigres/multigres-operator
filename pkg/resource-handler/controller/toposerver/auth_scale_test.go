package toposerver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func fallbackRootCluster(namespace, name string) *api.MultigresCluster {
	cluster := authGrantCluster("", name)
	cluster.Namespace = namespace
	cluster.Annotations = nil
	return cluster
}

func prefixGrant(name string, clusters []client.ObjectKey, prefixes ...string) etcdIdentityGrant {
	grant := etcdIdentityGrant{Name: name, Clusters: clusters}
	for _, prefix := range prefixes {
		grant.Permissions = append(grant.Permissions, etcdPrefixPermission(prefix))
	}
	return grant
}

func TestDesiredEtcdGrantsRefuseFallbackRoots(t *testing.T) {
	cellCluster := authGrantCluster("acme", "acme-cluster")
	zone1 := &api.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zone1", Namespace: cellCluster.Namespace,
			Labels: map[string]string{metadata.LabelMultigresCluster: cellCluster.Name},
		},
		Spec: api.CellSpec{
			Name: "zone1",
			GlobalTopoServer: api.GlobalTopoServerRef{
				Address: "https://shared.stores:2379", RootPath: "/multigres/acme/global",
			},
			TopoServer: &api.LocalTopoServerSpec{External: &api.ExternalTopoServerSpec{
				Endpoints: []api.EndpointUrl{"https://shared.stores:2379"},
				RootPath:  "/multigres/acme/zone1",
			}},
		},
	}
	tests := map[string]struct {
		objects     []client.Object
		wantRefused []client.ObjectKey
		wantGrants  []string
	}{
		"fallback root that would nest in a project global grant": {
			objects: []client.Object{
				authGrantCluster("acme", "acme-cluster"), fallbackRootCluster("acme", "global"),
			},
			wantRefused: []client.ObjectKey{{Namespace: "acme", Name: "global"}},
			wantGrants:  []string{"/multigres/acme"},
		},
		"cluster named after a cell": {
			objects:     []client.Object{cellCluster, zone1, fallbackRootCluster("acme", "zone1")},
			wantRefused: []client.ObjectKey{{Namespace: "acme", Name: "zone1"}},
			wantGrants:  []string{"/multigres/acme"},
		},
		"disjoint fallback root is still refused": {
			objects: []client.Object{
				authGrantCluster("acme", "acme-cluster"), fallbackRootCluster("acme", "other"),
			},
			wantRefused: []client.ObjectKey{{Namespace: "acme", Name: "other"}},
			wantGrants:  []string{"/multigres/acme"},
		},
		"refused clusters are reported in a stable order": {
			objects: []client.Object{
				fallbackRootCluster("b", "two"), fallbackRootCluster("a", "one"),
			},
			wantRefused: []client.ObjectKey{
				{Namespace: "a", Name: "one"},
				{Namespace: "b", Name: "two"},
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ts := &api.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
			}
			grants, refused, err := desiredEtcdGrants(
				t.Context(), authGrantReconciler(t, tt.objects...).Client, ts,
			)
			require.NoError(t, err)
			var refusedKeys []client.ObjectKey
			for _, refusal := range refused {
				refusedKeys = append(refusedKeys, refusal.cluster)
			}
			require.Equal(t, tt.wantRefused, refusedKeys)
			var names []string
			for _, grant := range grants {
				names = append(names, grant.Name)
			}
			require.Equal(t, tt.wantGrants, names)
		})
	}
}

func TestTopologyAuthStandaloneServerActivatesWithoutTenants(t *testing.T) {
	r, ts, cluster, auth := authReconcileFixture(t)
	require.NoError(t, r.Delete(t.Context(), cluster))
	r.MaintenanceClientFactory = func(context.Context, *api.TopoServer) (EtcdMaintenanceClient, error) {
		return &authGrantMaintenance{fakeEtcdAuth: auth}, nil
	}

	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.True(t, ts.Status.AuthEnabled)
	require.True(t, auth.enabled)
	require.Contains(t, auth.users, "root")
	require.Contains(t, auth.users, operatorEtcdUsername)
	require.Contains(t, auth.roles, "root")
	require.Empty(t, ts.Status.AuthIdentities)
	requireAuthorizationCondition(t, ts, metav1.ConditionTrue, "Reconciled")

	require.NoError(t, r.Create(t.Context(), authGrantCluster("late", "late")))
	ready, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, []string{"/multigres/late"}, ts.Status.AuthIdentities)
	require.Equal(t, []string{"/multigres/late"}, auth.users["/multigres/late"])
	require.Len(t, auth.roles["/multigres/late"], 1)
}

func TestTopologyAuthOwnedServerStillRequiresAnIdentity(t *testing.T) {
	r, ts, cluster, auth := authReconcileFixture(t)
	require.NoError(t, r.Delete(t.Context(), cluster))
	owner := authGrantCluster("owner", "owner")
	owner.Namespace = ts.Namespace
	ts.OwnerReferences = controllerRef("MultigresCluster", owner)

	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "no project identities resolved")
	require.False(t, ready)
	require.False(t, ts.Status.AuthEnabled)
	require.Empty(t, auth.mutations)
}

func TestReconcileEtcdAuthIsolatesIdentityFailuresOnceActive(t *testing.T) {
	grants := []etcdIdentityGrant{
		prefixGrant("bad", nil, "/bad/"),
		prefixGrant("good", nil, "/good/"),
	}
	for _, active := range []bool{true, false} {
		t.Run(map[bool]string{true: "active", false: "bootstrap"}[active], func(t *testing.T) {
			c := newFakeEtcdAuth()
			c.enabled = active
			c.failRoles = map[string]bool{"bad": true}
			result, err := reconcileEtcdAuth(t.Context(), c, grants, nil, authPassOptions{
				continueOnError: active,
			})
			if !active {
				require.Error(t, err)
				require.False(t, c.enabled)
				require.NotContains(t, c.users, "good")
				return
			}
			require.NoError(t, err)
			require.Contains(t, result.failed, "bad")
			require.Len(t, c.roles["good"], 1)
			require.Equal(t, []string{"good"}, c.users["good"])
		})
	}
}

func TestReconcileEtcdAuthBoundsEachIdentity(t *testing.T) {
	c := newFakeEtcdAuth()
	c.enabled = true
	c.blockRoles = map[string]bool{"slow": true}
	start := time.Now()
	result, err := reconcileEtcdAuth(t.Context(), c, []etcdIdentityGrant{
		prefixGrant("slow", nil, "/slow/"),
		prefixGrant("fast", nil, "/fast/"),
	}, nil, authPassOptions{continueOnError: true, identityTimeout: 50 * time.Millisecond})
	require.NoError(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
	require.ErrorIs(t, result.failed["slow"], context.DeadlineExceeded)
	require.Len(t, c.roles["fast"], 1, "a slow identity must not starve the ones after it")
}

func TestTopologyAuthPartialFailureAfterActivationKeepsTracking(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)

	require.NoError(t, r.Create(t.Context(), authGrantCluster("bad", "bad")))
	require.NoError(t, r.Create(t.Context(), authGrantCluster("good", "good")))
	auth.failRoles = map[string]bool{"/multigres/bad": true}
	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "1 of 3 etcd identities failed to reconcile")
	require.ErrorContains(t, err, "/multigres/bad")
	require.False(t, ready)
	require.True(t, ts.Status.AuthEnabled)
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "IdentitiesFailed")
	require.Len(t, auth.roles["/multigres/good"], 1)
	require.Equal(
		t,
		[]string{"/multigres/bad", "/multigres/good", "/multigres/project"},
		ts.Status.AuthIdentities,
		"a failed identity stays tracked",
	)

	auth.failRoles = nil
	ready, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.Len(t, auth.roles["/multigres/bad"], 1)
}

func TestTopologyAuthFailedPruneStaysTracked(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	second := authGrantCluster("second", "second")
	require.NoError(t, r.Create(t.Context(), second))
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.NoError(t, r.Delete(t.Context(), second))
	r.APIReader = authConfirmationReader(t, r, authGrantCluster("project", "cluster"))

	auth.failRoleDelete = true
	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "etcd identities failed to reconcile")
	require.False(t, ready)
	require.Equal(t, []string{"/multigres/project", "/multigres/second"}, ts.Status.AuthIdentities)
	require.Contains(t, ts.Status.AuthIdentityClusters, "/multigres/second")

	auth.failRoleDelete = false
	ready, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, []string{"/multigres/project"}, ts.Status.AuthIdentities)
	require.NotContains(t, auth.roles, "/multigres/second")
}

func TestStandaloneDiscoveryAvoidsFullLists(t *testing.T) {
	shared := authGrantCluster("shared", "shared")
	unrelated := authGrantCluster("other", "other")
	unrelated.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{
		"https://elsewhere.stores:2379",
	}
	viaCell := authGrantCluster("via-cell", "via-cell")
	viaCell.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{
		"https://elsewhere.stores:2379",
	}
	cell := &api.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zone", Namespace: viaCell.Namespace,
			Labels: map[string]string{metadata.LabelMultigresCluster: viaCell.Name},
		},
		Spec: api.CellSpec{
			Name: "zone",
			GlobalTopoServer: api.GlobalTopoServerRef{
				Address: "https://shared.stores:2379", RootPath: "/multigres/via-cell/global",
			},
		},
	}
	base := authGrantReconciler(t, shared, unrelated, viaCell, cell).Client
	fullLists := 0
	reader := interceptor.NewClient(
		base.(client.WithWatch),
		interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				listOptions := &client.ListOptions{}
				listOptions.ApplyOptions(opts)
				if listOptions.FieldSelector == nil && listOptions.LabelSelector == nil {
					fullLists++
				}
				return c.List(ctx, list, opts...)
			},
		},
	)
	ts := &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
		Status:     api.TopoServerStatus{AuthEnabled: true},
	}

	grants, _, err := desiredEtcdGrants(t.Context(), reader, ts)
	require.NoError(t, err)
	require.Zero(t, fullLists, "an activated server must discover tenants through its indexes")
	names := make([]string, 0, len(grants))
	for _, grant := range grants {
		names = append(names, grant.Name)
	}
	require.Equal(t, []string{"/multigres/shared", "/multigres/via-cell"}, names)
}

func TestClusterTopologyServerKeys(t *testing.T) {
	cluster := authGrantCluster("p", "c")
	cluster.Spec.Cells = []api.CellConfig{
		{Name: "a", Spec: &api.CellInlineSpec{LocalTopoServer: &api.LocalTopoServerSpec{
			External: &api.ExternalTopoServerSpec{
				Endpoints: []api.EndpointUrl{"https://local.stores:2379"},
			},
		}}},
		{Name: "b", CellTemplate: "template"},
	}
	require.Equal(
		t,
		[]string{"stores/shared", "stores/local"},
		clusterTopologyServerKeys(cluster),
	)
	require.Nil(t, clusterTopologyServerKeys(&api.Cell{}))
}

func TestDeletedClusterEnqueuesAuthManagedServersItReferenced(t *testing.T) {
	managed := func(name string) *api.TopoServer {
		return &api.TopoServer{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "stores"},
			Spec: api.TopoServerSpec{
				Auth: &api.TopoServerAuthSpec{Enabled: true},
				Etcd: &api.EtcdSpec{},
				TLS:  &api.TopoTLSConfig{Enabled: ptr.To(true)},
			},
		}
	}
	plain := &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "stores"},
		Spec:       api.TopoServerSpec{Etcd: &api.EtcdSpec{}},
	}
	cluster := authGrantCluster("p", "c")
	cluster.Spec.Cells = []api.CellConfig{
		{Name: "inline", Spec: &api.CellInlineSpec{LocalTopoServer: &api.LocalTopoServerSpec{
			External: &api.ExternalTopoServerSpec{
				Endpoints: []api.EndpointUrl{"https://cell-local.stores:2379"},
			},
		}}},
		{Name: "templated", CellTemplate: "external"},
		{Name: "unreadable", CellTemplate: "missing"},
		{Name: "plain", Spec: &api.CellInlineSpec{LocalTopoServer: &api.LocalTopoServerSpec{
			External: &api.ExternalTopoServerSpec{
				Endpoints: []api.EndpointUrl{"https://plain.stores:2379"},
			},
		}}},
	}
	r := authGrantReconciler(
		t,
		managed("shared"), managed("cell-local"), managed("from-template"), plain,
		&api.CellTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "external", Namespace: cluster.Namespace},
			Spec: api.CellTemplateSpec{LocalTopoServer: &api.LocalTopoServerSpec{
				External: &api.ExternalTopoServerSpec{
					Endpoints: []api.EndpointUrl{"https://from-template.stores:2379"},
				},
			}},
		},
	)

	requests := r.authManagedTopoServersForCluster(t.Context(), cluster)
	got := make([]string, 0, len(requests))
	for _, request := range requests {
		got = append(got, request.String())
	}
	require.ElementsMatch(t, []string{
		"stores/shared", "stores/cell-local", "stores/from-template",
	}, got)
	require.Empty(t, r.authManagedTopoServersForCluster(t.Context(), &api.Cell{}))
}
