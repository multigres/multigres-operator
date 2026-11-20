package toposerver

import (
	"context"
	"slices"
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

func TestGrantConflicts(t *testing.T) {
	a := []client.ObjectKey{{Namespace: "ns", Name: "a"}}
	b := []client.ObjectKey{{Namespace: "ns", Name: "b"}}
	tests := map[string]struct {
		grants []etcdIdentityGrant
		want   []string
	}{
		"no conflict": {
			grants: []etcdIdentityGrant{
				prefixGrant("/multigres/one", a, "/multigres/one/global/"),
				prefixGrant("/multigres/two", b, "/multigres/two/global/"),
			},
		},
		"sibling prefixes do not conflict": {
			grants: []etcdIdentityGrant{
				prefixGrant("/multigres/a", a, "/multigres/a/"),
				prefixGrant("/multigres/ab", b, "/multigres/ab/"),
			},
		},
		"identical ranges under different identities": {
			grants: []etcdIdentityGrant{
				prefixGrant("/multigres/one", a, "/multigres/shared/"),
				prefixGrant("/multigres/two", b, "/multigres/shared/"),
			},
			want: []string{"cluster ns/a (/multigres/one) and cluster ns/b (/multigres/two)"},
		},
		"nested ranges": {
			grants: []etcdIdentityGrant{
				prefixGrant("/multigres/acme", a, "/multigres/acme/global/"),
				prefixGrant("/multigres/acme/global", b, "/multigres/acme/global/global/"),
			},
			want: []string{
				"cluster ns/a (/multigres/acme) and cluster ns/b (/multigres/acme/global)",
			},
		},
		"clusters sharing one identity are one project": {
			grants: []etcdIdentityGrant{
				prefixGrant(
					"/multigres/acme",
					append(slices.Clone(a), b...),
					"/multigres/acme/global/",
				),
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var got []string
			for _, conflict := range grantConflicts(tt.grants) {
				got = append(got, conflict.String())
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDesiredEtcdGrantsConflictsFromSharedRootNamespace(t *testing.T) {
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
		objects []client.Object
		want    string
	}{
		"fallback root nested in a project global grant": {
			objects: []client.Object{
				authGrantCluster("acme", "acme-cluster"), fallbackRootCluster("acme", "global"),
			},
			want: "cluster projects/acme-cluster (/multigres/acme) and cluster acme/global (/multigres/acme/global)",
		},
		"cluster named after a cell": {
			objects: []client.Object{cellCluster, zone1, fallbackRootCluster("acme", "zone1")},
			want:    "cluster projects/acme-cluster (/multigres/acme) and cluster acme/zone1 (/multigres/acme/zone1)",
		},
		"disjoint roots": {
			objects: []client.Object{
				authGrantCluster("acme", "acme-cluster"), fallbackRootCluster("acme", "other"),
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ts := &api.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
			}
			grants, err := desiredEtcdGrants(
				t.Context(), authGrantReconciler(t, tt.objects...).Client, ts,
			)
			require.NoError(t, err)
			conflicts := grantConflicts(grants)
			if tt.want == "" {
				require.Empty(t, conflicts)
				return
			}
			require.Len(t, conflicts, 1)
			require.Equal(t, tt.want, conflicts[0].String())
		})
	}
}

func TestTopologyAuthConflictBeforeActivationWritesNothing(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	require.NoError(t, r.Create(t.Context(), fallbackRootCluster("project", "global")))
	clients := 0
	factory := r.MaintenanceClientFactory
	r.MaintenanceClientFactory = func(ctx context.Context, ts *api.TopoServer) (EtcdMaintenanceClient, error) {
		clients++
		return factory(ctx, ts)
	}

	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "projects/cluster")
	require.ErrorContains(t, err, "project/global")
	require.False(t, ready)
	require.False(t, ts.Status.AuthEnabled)
	require.Zero(t, clients)
	require.Empty(t, auth.mutations)
	require.Empty(t, ts.Status.AuthIdentities)
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "IdentityConflict")
}

func TestTopologyAuthConflictAfterActivationKeepsExistingGrants(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	tracked := slices.Clone(ts.Status.AuthIdentities)
	require.Equal(t, []string{"/multigres/project"}, tracked)
	roleBefore := slices.Clone(auth.roles["/multigres/project"])

	require.NoError(t, r.Create(t.Context(), fallbackRootCluster("project", "global")))
	auth.mutations = nil
	ready, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "topology roots overlap")
	require.False(t, ready)
	require.True(t, ts.Status.AuthEnabled)
	require.True(t, auth.enabled)
	require.Empty(t, auth.mutations, "neither side of the conflict may change")
	require.Equal(t, roleBefore, auth.roles["/multigres/project"])
	require.NotContains(t, auth.users, "/multigres/project/global")
	require.Equal(t, tracked, ts.Status.AuthIdentities)
	requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "IdentityConflict")
}

func TestTopologyAuthConflictAfterActivationStillReconcilesOthers(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)

	require.NoError(t, r.Create(t.Context(), fallbackRootCluster("project", "global")))
	require.NoError(t, r.Create(t.Context(), authGrantCluster("bystander", "bystander")))
	_, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, "topology roots overlap")
	require.Equal(t, []string{"/multigres/bystander"}, auth.users["/multigres/bystander"])
	require.Equal(
		t,
		[]string{"/multigres/bystander", "/multigres/project"},
		ts.Status.AuthIdentities,
	)
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
	ts := &api.TopoServer{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"}}

	grants, err := desiredEtcdGrants(t.Context(), reader, ts)
	require.NoError(t, err)
	require.Zero(t, fullLists, "a standalone server must discover tenants through its indexes")
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

	requests := r.authManagedTopoServersForDeletedCluster(t.Context(), cluster)
	got := make([]string, 0, len(requests))
	for _, request := range requests {
		got = append(got, request.String())
	}
	require.ElementsMatch(t, []string{
		"stores/shared", "stores/cell-local", "stores/from-template",
	}, got)
	require.Empty(t, r.authManagedTopoServersForDeletedCluster(t.Context(), &api.Cell{}))
}
