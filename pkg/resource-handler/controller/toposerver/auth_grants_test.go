package toposerver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/topology"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func authGrantCluster(project, name string) *api.MultigresCluster {
	return &api.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "projects", UID: types.UID("cluster-" + name),
			Annotations: map[string]string{metadata.AnnotationProjectRef: project},
		},
		Spec: api.MultigresClusterSpec{
			TopoTLS: &api.TopoTLSConfig{Enabled: ptr.To(true)},
			GlobalTopoServer: &api.GlobalTopoServerSpec{
				External: &api.ExternalTopoServerSpec{
					Endpoints: []api.EndpointUrl{"https://shared.stores:2379"},
				},
			},
		},
	}
}

func authGrantReconciler(t *testing.T, objects ...client.Object) *TopoServerReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&api.TopoServer{}).
		WithIndex(&api.Cell{}, topologyServerCellIndex, cellTopologyServerKeys).
		WithIndex(&api.MultigresCluster{}, clusterTopologyServerIndex, clusterTopologyServerKeys).
		WithObjects(objects...).Build()
	return &TopoServerReconciler{Client: c, APIReader: c}
}

func controllerRef(kind string, owner client.Object) []metav1.OwnerReference {
	return []metav1.OwnerReference{{
		APIVersion: api.GroupVersion.String(),
		Kind:       kind,
		Name:       owner.GetName(),
		UID:        owner.GetUID(),
		Controller: ptr.To(true),
	}}
}

func TestDesiredEtcdGrantsUnionAndProjectIsolation(t *testing.T) {
	cluster := authGrantCluster("proj_123", "one")
	roots, err := topology.ForCluster(cluster)
	require.NoError(t, err)
	objects := []client.Object{cluster, authGrantCluster("proj_1234", "two")}
	for _, name := range []string{"a", "b"} {
		root, err := roots.Cell(name)
		require.NoError(t, err)
		objects = append(objects, &api.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: cluster.Namespace,
				Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
			},
			Spec: api.CellSpec{
				Name: api.CellName(name),
				GlobalTopoServer: api.GlobalTopoServerRef{
					Address: "https://shared.stores:2379", RootPath: roots.Global(),
				},
				TopoServer: &api.LocalTopoServerSpec{
					External: &api.ExternalTopoServerSpec{
						Endpoints: []api.EndpointUrl{"https://shared.stores:2379"},
						RootPath:  root,
					},
				},
			},
		})
	}
	r := authGrantReconciler(t, objects...)
	grants, _, err := desiredEtcdGrants(context.Background(), r.Client, &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
	})
	require.NoError(t, err)
	require.Len(t, grants, 2)
	for _, grant := range grants {
		seen := map[etcdPermission]bool{}
		for _, permission := range grant.Permissions {
			require.False(t, seen[permission])
			seen[permission] = true
			require.Equal(t, clientv3.GetPrefixRangeEnd(permission.Key), permission.RangeEnd)
			require.True(t, strings.HasSuffix(permission.Key, "/"))
		}
		if grant.Name != roots.ClusterRoot() {
			continue
		}
		require.Len(t, grant.Permissions, 3)
		for _, permission := range grant.Permissions {
			require.True(t, strings.HasPrefix(permission.Key, roots.KeyPrefix()))
			require.Less(t, permission.RangeEnd, "/multigres/proj_1234/")
			require.Equal(t, clientv3.PermReadWrite, permission.Type)
		}
	}
}

func TestDesiredEtcdGrantsShapes(t *testing.T) {
	sharedServer := &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
	}
	key := func(name string) client.ObjectKey {
		return client.ObjectKey{Namespace: "projects", Name: name}
	}
	inlineZone := func(rootPath string) []api.CellConfig {
		return []api.CellConfig{{
			Name: "zone",
			Spec: &api.CellInlineSpec{LocalTopoServer: &api.LocalTopoServerSpec{
				External: &api.ExternalTopoServerSpec{
					Endpoints: []api.EndpointUrl{"https://shared.stores:2379"},
					RootPath:  rootPath,
				},
			}},
		}}
	}
	withCells := func(cluster *api.MultigresCluster, cells []api.CellConfig) *api.MultigresCluster {
		cluster.Spec.Cells = cells
		return cluster
	}
	conventionalName := authGrantCluster("project", "cluster")
	conventionalName.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{Etcd: &api.EtcdSpec{
		RootPath: "/multigres/project/global",
	}}
	invalidRoot := authGrantCluster("one", "one")
	invalidRoot.Spec.GlobalTopoServer.External.RootPath = "/multigres/other/global"

	tests := map[string]struct {
		objects     []client.Object
		ts          *api.TopoServer
		want        []etcdIdentityGrant
		wantRefusal string
	}{
		"project identity": {
			objects: []client.Object{authGrantCluster("project", "one")},
			want: []etcdIdentityGrant{{
				Name:        "/multigres/project",
				Clusters:    []client.ObjectKey{key("one")},
				Permissions: []etcdPermission{etcdPrefixPermission("/multigres/project/global/")},
			}},
		},
		"includes cell root": {
			objects: []client.Object{
				withCells(
					authGrantCluster("project", "one"),
					inlineZone("/multigres/project/zone"),
				),
			},
			want: []etcdIdentityGrant{{
				Name:     "/multigres/project",
				Clusters: []client.ObjectKey{key("one")},
				Permissions: []etcdPermission{
					etcdPrefixPermission("/multigres/project/global/"),
					etcdPrefixPermission("/multigres/project/zone/"),
				},
			}},
		},
		"shared project and colocated roots": {
			objects: []client.Object{
				withCells(
					authGrantCluster("project", "one"),
					inlineZone("/multigres/project/global"),
				),
				authGrantCluster("project", "two"),
			},
			want: []etcdIdentityGrant{{
				Name:        "/multigres/project",
				Clusters:    []client.ObjectKey{key("one"), key("two")},
				Permissions: []etcdPermission{etcdPrefixPermission("/multigres/project/global/")},
			}},
		},
		"refuses invalid referenced root": {
			objects:     []client.Object{invalidRoot},
			want:        []etcdIdentityGrant{},
			wantRefusal: `has topology root "/multigres/other/global" outside project prefix "/multigres/one/"`,
		},
		"does not match managed server by conventional name": {
			objects: []client.Object{conventionalName},
			ts: &api.TopoServer{ObjectMeta: metav1.ObjectMeta{
				Name: "cluster-global-topo", Namespace: "projects",
			}},
			want: []etcdIdentityGrant{},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ts := tt.ts
			if ts == nil {
				ts = sharedServer
			}
			grants, refused, err := desiredEtcdGrants(
				t.Context(), authGrantReconciler(t, tt.objects...).Client, ts,
			)
			require.NoError(t, err)
			require.Equal(t, tt.want, grants)
			if tt.wantRefusal != "" {
				require.Len(t, refused, 1)
				require.Contains(t, refused[0].reason, tt.wantRefusal)
			}
		})
	}
}

func TestTopologyAuthRejectsUnmanagedClientIdentities(t *testing.T) {
	for _, scenario := range []string{"global client secret", "TLS disabled", "desired local endpoint", "live local endpoint"} {
		t.Run(scenario, func(t *testing.T) {
			cluster := authGrantCluster("custom", "custom")
			global := cluster.Spec.GlobalTopoServer.External
			global.ClientCertSecret = "custom-client"
			objects := []client.Object{authGrantCluster("valid", "valid"), cluster}
			switch scenario {
			case "TLS disabled":
				cluster.Spec.TopoTLS = nil
				global.ClientCertSecret = ""
			case "desired local endpoint", "live local endpoint":
				global.Endpoints = []api.EndpointUrl{"https://other.stores:2379"}
				local := &api.LocalTopoServerSpec{External: &api.ExternalTopoServerSpec{
					Endpoints: []api.EndpointUrl{"https://shared.stores:2379"},
					RootPath:  "/multigres/custom/zone",
				}}
				if scenario == "desired local endpoint" {
					cluster.Spec.Cells = []api.CellConfig{{
						Name: "zone",
						Spec: &api.CellInlineSpec{LocalTopoServer: local},
					}}
				} else {
					objects = append(objects, &api.Cell{
						ObjectMeta: metav1.ObjectMeta{
							Name: "zone", Namespace: cluster.Namespace,
							Labels: map[string]string{
								metadata.LabelMultigresCluster: cluster.Name,
							},
						},
						Spec: api.CellSpec{Name: "zone", TopoServer: local},
					})
				}
			}
			r := authGrantReconciler(t, objects...)
			r.MaintenanceClientFactory = func(context.Context, *api.TopoServer) (EtcdMaintenanceClient, error) {
				t.Fatal(
					"must not contact etcd when a referencing cluster lacks a managed project credential",
				)
				return nil, nil
			}
			ts := &api.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
				Spec: api.TopoServerSpec{
					Auth: &api.TopoServerAuthSpec{Enabled: true},
					Etcd: &api.EtcdSpec{},
					TLS:  &api.TopoTLSConfig{Enabled: ptr.To(true)},
				},
			}
			ready, err := r.reconcileTopologyAuth(context.Background(), ts)
			require.ErrorContains(
				t,
				err,
				"an operator-issued topology client certificate is required",
			)
			require.False(t, ready)
			require.False(t, ts.Status.AuthEnabled)
		})
	}
}

func TestDesiredEtcdGrantsManagedOwner(t *testing.T) {
	for _, scenario := range []string{"cluster owns global", "cell owns local"} {
		t.Run(scenario, func(t *testing.T) {
			cluster := authGrantCluster("project", "one")
			cell := &api.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name: "zone", Namespace: cluster.Namespace, UID: "cell-zone",
					Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
				},
				Spec: api.CellSpec{
					Name: "zone",
					TopoServer: &api.LocalTopoServerSpec{
						Etcd: &api.EtcdSpec{RootPath: "/multigres/project/zone"},
					},
				},
			}
			ts := &api.TopoServer{ObjectMeta: metav1.ObjectMeta{
				Name: "managed", Namespace: cluster.Namespace,
			}}
			wantPrefix := "/multigres/project/zone/"
			if scenario == "cluster owns global" {
				cluster.Spec.GlobalTopoServer = nil
				ts.OwnerReferences = controllerRef("MultigresCluster", cluster)
				wantPrefix = "/multigres/project/global/"
			} else {
				ts.OwnerReferences = controllerRef("Cell", cell)
			}
			grants, _, err := desiredEtcdGrants(
				t.Context(), authGrantReconciler(t, cluster, cell).Client, ts,
			)
			require.NoError(t, err)
			require.Len(t, grants, 1)
			for _, grant := range grants {
				for _, permission := range grant.Permissions {
					require.True(t, strings.HasPrefix(permission.Key, wantPrefix), permission.Key)
				}
			}
		})
	}
}

func TestDesiredEtcdGrantsRetainsTerminatingReferences(t *testing.T) {
	for _, scenario := range []string{"cluster", "global cell", "external local cell", "owned local cell"} {
		t.Run(scenario, func(t *testing.T) {
			cluster := authGrantCluster("project", "one")
			cell := &api.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name: "zone", Namespace: cluster.Namespace, UID: "cell-zone",
					Labels: map[string]string{
						metadata.LabelMultigresCluster: cluster.Name,
					},
				},
			}
			ts := &api.TopoServer{ObjectMeta: metav1.ObjectMeta{
				Name: "shared", Namespace: "stores",
			}}
			var terminating client.Object = cell
			switch scenario {
			case "cluster":
				terminating = cluster
			case "global cell":
				cell.Spec.GlobalTopoServer = api.GlobalTopoServerRef{
					Address:  "shared.stores:2379",
					RootPath: "/multigres/project/old-global",
				}
			case "external local cell":
				cell.Spec.TopoServer = &api.LocalTopoServerSpec{
					External: &api.ExternalTopoServerSpec{
						Endpoints: []api.EndpointUrl{"shared.stores:2379"},
						RootPath:  "/multigres/project/zone",
					},
				}
			case "owned local cell":
				ts.Namespace = cluster.Namespace
				ts.OwnerReferences = controllerRef("Cell", cell)
				cell.Spec.TopoServer = &api.LocalTopoServerSpec{
					Etcd: &api.EtcdSpec{RootPath: "/multigres/project/zone"},
				}
			}
			terminating.SetFinalizers([]string{"test.example/cleanup"})
			r := authGrantReconciler(t, cluster, cell)
			before, _, err := desiredEtcdGrants(t.Context(), r.Client, ts)
			require.NoError(t, err)
			require.Len(t, before, 1)
			require.NoError(t, r.Delete(t.Context(), terminating))
			after, _, err := desiredEtcdGrants(t.Context(), r.Client, ts)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.NoError(
				t,
				r.Get(t.Context(), client.ObjectKeyFromObject(terminating), terminating),
			)
			terminating.SetFinalizers(nil)
			require.NoError(t, r.Update(t.Context(), terminating))
			after, _, err = desiredEtcdGrants(t.Context(), r.Client, ts)
			require.NoError(t, err)
			require.NotEqual(
				t,
				before,
				after,
				"grants retire after the referencing object disappears",
			)
		})
	}
}

func TestDesiredEtcdGrantsRejectsTerminatingOrphanCells(t *testing.T) {
	for _, scenario := range []string{"global", "external local", "owned local"} {
		t.Run(scenario, func(t *testing.T) {
			cell := &api.Cell{ObjectMeta: metav1.ObjectMeta{
				Name:       "orphan",
				Namespace:  "stores",
				UID:        "cell-orphan",
				Finalizers: []string{"test.example/cleanup"},
			}}
			ts := &api.TopoServer{ObjectMeta: metav1.ObjectMeta{
				Name: "shared", Namespace: "stores",
			}}
			switch scenario {
			case "global":
				cell.Spec.GlobalTopoServer.Address = "shared.stores:2379"
			case "external local":
				cell.Spec.TopoServer = &api.LocalTopoServerSpec{
					External: &api.ExternalTopoServerSpec{
						Endpoints: []api.EndpointUrl{"shared.stores:2379"},
					},
				}
			case "owned local":
				ts.OwnerReferences = controllerRef("Cell", cell)
			}
			r := authGrantReconciler(t, cell)
			require.NoError(t, r.Delete(t.Context(), cell))
			grants, _, err := desiredEtcdGrants(t.Context(), r.Client, ts)
			require.ErrorContains(t, err, "cannot resolve project identity")
			require.Nil(t, grants)
		})
	}
}

func TestDesiredEtcdGrantsIgnoresUnrelatedBrokenClusters(t *testing.T) {
	for _, scenario := range []string{"missing global template", "missing cell template", "invalid project identity"} {
		t.Run(scenario, func(t *testing.T) {
			valid := authGrantCluster("valid", "valid")
			broken := authGrantCluster("broken", "broken")
			broken.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{
				"https://other.stores:2379",
			}
			switch scenario {
			case "missing global template":
				broken.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{TemplateRef: "missing"}
			case "missing cell template":
				broken.Spec.Cells = []api.CellConfig{{Name: "zone", CellTemplate: "missing"}}
			case "invalid project identity":
				broken.Annotations[metadata.AnnotationProjectRef] = "broken\x00identity"
			}

			grants, _, err := desiredEtcdGrants(
				t.Context(),
				authGrantReconciler(t, valid, broken).Client,
				&api.TopoServer{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"}},
			)
			require.NoError(t, err)
			require.Len(t, grants, 1)
			require.Equal(t, "/multigres/valid", grants[0].Name)
		})
	}
}

func TestDesiredEtcdGrantsFailsClosedForAssociatedBrokenCluster(t *testing.T) {
	cluster := authGrantCluster("broken", "broken")
	cluster.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{TemplateRef: "missing"}
	cell := &api.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name: "broken-zone", Namespace: cluster.Namespace,
			Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
		},
		Spec: api.CellSpec{
			Name: "zone",
			GlobalTopoServer: api.GlobalTopoServerRef{
				Address: "shared.stores:2379", RootPath: "/multigres/broken/global",
			},
		},
	}

	grants, _, err := desiredEtcdGrants(
		t.Context(),
		authGrantReconciler(t, cluster, cell).Client,
		&api.TopoServer{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"}},
	)
	require.ErrorContains(t, err, "resolving cluster broken topology")
	require.Nil(t, grants)
}

func TestDesiredEtcdGrantsUsesMaterializedCellsWithoutResolvingTemplates(t *testing.T) {
	cluster := authGrantCluster("project", "cluster")
	cluster.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{
		"https://other.stores:2379",
	}
	cluster.Spec.Cells = []api.CellConfig{{Name: "zone", CellTemplate: "must-not-be-read"}}
	cell := &api.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cluster-zone", Namespace: cluster.Namespace,
			Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
		},
		Spec: api.CellSpec{
			Name: "zone",
			TopoServer: &api.LocalTopoServerSpec{External: &api.ExternalTopoServerSpec{
				Endpoints: []api.EndpointUrl{"shared.stores:2379"},
				RootPath:  "/multigres/project/zone",
			}},
		},
	}
	r := authGrantReconciler(t, cluster, cell)
	templateGets := 0
	reader := testutil.NewFakeClientWithFailures(r.Client, &testutil.FailureConfig{
		OnGet: func(key client.ObjectKey) error {
			if key.Name == "must-not-be-read" {
				templateGets++
				return errors.New("materialized cell template was read")
			}
			return nil
		},
	})

	grants, _, err := desiredEtcdGrants(
		t.Context(), reader,
		&api.TopoServer{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"}},
	)
	require.NoError(t, err)
	require.Zero(t, templateGets)
	require.Len(t, grants, 1)
	require.Equal(t, "/multigres/project/zone/", grants[0].Permissions[0].Key)
}

func TestDesiredEtcdGrantsManagedServerScopesDiscoveryToOwner(t *testing.T) {
	owner := authGrantCluster("owner", "owner")
	owner.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{Etcd: &api.EtcdSpec{
		RootPath: "/multigres/owner/global",
	}}
	unrelated := authGrantCluster("unrelated", "unrelated")
	unrelated.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{TemplateRef: "must-not-be-read"}
	ts := &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed", Namespace: owner.Namespace,
			OwnerReferences: controllerRef("MultigresCluster", owner),
		},
		Status: api.TopoServerStatus{AuthEnabled: true},
	}
	r := authGrantReconciler(t, owner, unrelated)
	clusterLists, cellLists := 0, 0
	reader := testutil.NewFakeClientWithFailures(r.Client, &testutil.FailureConfig{
		OnGet: func(key client.ObjectKey) error {
			if key.Name == "must-not-be-read" {
				return errors.New("unrelated template was read")
			}
			return nil
		},
		OnList: func(list client.ObjectList) error {
			switch list.(type) {
			case *api.MultigresClusterList:
				clusterLists++
				return errors.New("managed discovery must not list clusters")
			case *api.CellList:
				cellLists++
			}
			return nil
		},
	})

	grants, _, err := desiredEtcdGrants(t.Context(), reader, ts)
	require.NoError(t, err)
	require.Len(t, grants, 1)
	require.Equal(t, "/multigres/owner", grants[0].Name)
	require.Zero(t, clusterLists)
	require.Equal(t, 2, cellLists)
}

func TestDesiredEtcdGrantsManagedServerIncludesReferencingCluster(t *testing.T) {
	owner := authGrantCluster("owner", "owner")
	owner.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{Etcd: &api.EtcdSpec{
		RootPath: "/multigres/owner/global",
	}}
	referencing := authGrantCluster("guest", "guest")
	referencing.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{
		"https://managed.projects.svc:2379",
	}
	referencing.Spec.GlobalTopoServer.External.RootPath = "/multigres/guest/global"
	cell := &api.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "guest-zone",
			Namespace: referencing.Namespace,
			Labels:    map[string]string{metadata.LabelMultigresCluster: referencing.Name},
			OwnerReferences: controllerRef(
				"MultigresCluster",
				referencing,
			),
		},
		Spec: api.CellSpec{
			Name: "guest-zone",
			GlobalTopoServer: api.GlobalTopoServerRef{
				Address:  "https://managed.projects.svc:2379",
				RootPath: "/multigres/guest/global",
			},
		},
	}
	ts := &api.TopoServer{ObjectMeta: metav1.ObjectMeta{
		Name: "managed", Namespace: owner.Namespace,
		OwnerReferences: controllerRef("MultigresCluster", owner),
	}}

	grants, _, err := desiredEtcdGrants(
		t.Context(),
		authGrantReconciler(t, owner, referencing, cell).Client,
		ts,
	)
	require.NoError(t, err)
	require.Len(t, grants, 2)
	require.Equal(t, "/multigres/guest", grants[0].Name)
	require.Equal(t, "/multigres/owner", grants[1].Name)
}

func TestActivationDiscoversReferencingClustersWithoutCells(t *testing.T) {
	template := &api.CellTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "cells", Namespace: "projects"},
		Spec: api.CellTemplateSpec{LocalTopoServer: &api.LocalTopoServerSpec{
			External: &api.ExternalTopoServerSpec{
				Endpoints: []api.EndpointUrl{"https://shared.stores:2379"},
			},
		}},
	}
	viaTemplate := func(project string, explicit bool) *api.MultigresCluster {
		cluster := authGrantCluster(project, "tenant")
		cluster.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{
			"https://other.stores:2379",
		}
		cluster.Spec.Cells = []api.CellConfig{{Name: "zone"}}
		if explicit {
			cluster.Spec.Cells[0].CellTemplate = "cells"
		} else {
			cluster.Spec.TemplateDefaults.CellTemplate = "cells"
		}
		return cluster
	}
	withoutRef := viaTemplate("", false)
	withoutRef.Annotations = nil
	owner := authGrantCluster("owner", "owner")
	owner.Namespace = "stores"
	owner.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{Etcd: &api.EtcdSpec{
		RootPath: "/multigres/owner/global",
	}}
	for name, tc := range map[string]struct {
		cluster     *api.MultigresCluster
		owned       bool
		wantGrants  []string
		wantRefused []clusterRefusal
	}{
		"default cell template": {
			cluster:    viaTemplate("tenant", false),
			wantGrants: []string{"/multigres/tenant"},
		},
		"explicit cell template": {
			cluster:    viaTemplate("tenant", true),
			wantGrants: []string{"/multigres/tenant"},
		},
		"cell template without a project ref": {
			cluster: withoutRef,
			wantRefused: []clusterRefusal{{
				cluster: client.ObjectKeyFromObject(withoutRef),
				reason:  "has no " + metadata.AnnotationProjectRef + " annotation",
			}},
		},
		"inline global reference to an owned server": {
			cluster:    authGrantCluster("guest", "guest"),
			owned:      true,
			wantGrants: []string{"/multigres/guest", "/multigres/owner"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ts := &api.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
			}
			if tc.owned {
				ts.OwnerReferences = controllerRef("MultigresCluster", owner)
			}
			reader := authGrantReconciler(t, tc.cluster, template, owner).Client
			grants, refused, err := desiredEtcdGrants(t.Context(), reader, ts)
			require.NoError(t, err)
			var names []string
			for _, grant := range grants {
				names = append(names, grant.Name)
			}
			require.Equal(t, tc.wantGrants, names)
			require.Equal(t, tc.wantRefused, refused)
		})
	}
}

func TestActivationBlocksOnUnresolvedInlineTopologyReference(t *testing.T) {
	for name, configure := range map[string]func(*api.MultigresCluster){
		"global": func(cluster *api.MultigresCluster) {
			cluster.Spec.GlobalTopoServer.TemplateRef = "missing"
		},
		"local": func(cluster *api.MultigresCluster) {
			cluster.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{"other.stores:2379"}
			cluster.Spec.Cells = []api.CellConfig{{
				Name:         "zone",
				CellTemplate: "missing",
				Spec: &api.CellInlineSpec{LocalTopoServer: &api.LocalTopoServerSpec{
					External: &api.ExternalTopoServerSpec{
						Endpoints: []api.EndpointUrl{"shared.stores:2379"},
					},
				}},
			}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			r, ts, _, auth := authReconcileFixture(t)
			cluster := authGrantCluster("unresolved", "unresolved")
			configure(cluster)
			require.NoError(t, r.Create(ctx, cluster))

			ready, err := r.reconcileTopologyAuth(ctx, ts)
			require.ErrorContains(t, err, "missing")
			require.False(t, ready)
			require.False(t, ts.Status.AuthEnabled)
			require.Empty(t, auth.mutations)
		})
	}
}

func TestActivationRequiresUncachedDiscovery(t *testing.T) {
	for _, noReader := range []bool{false, true} {
		name := "uncached tenant omitted from cache"
		if noReader {
			name = "no uncached reader"
		}
		t.Run(name, func(t *testing.T) {
			r, ts, known, auth := authReconcileFixture(t)
			wantError := "without an uncached API reader"
			if noReader {
				r.APIReader = nil
			} else {
				hidden := authGrantCluster("", "hidden")
				scheme := runtime.NewScheme()
				require.NoError(t, api.AddToScheme(scheme))
				r.APIReader = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(known, hidden).
					Build()
				wantError = "projects/hidden"
			}
			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.ErrorContains(t, err, wantError)
			require.False(t, ready)
			require.False(t, ts.Status.AuthEnabled)
			require.Empty(t, ts.Status.AuthIdentities)
			require.Empty(t, auth.mutations)
		})
	}
}

func TestReactivationRequiresUncachedDiscovery(t *testing.T) {
	for _, newCluster := range []bool{false, true} {
		name := "existing etcd disabled"
		if newCluster {
			name = "replacement etcd"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			r, ts, known, auth := authReconcileFixture(t)
			ready, err := r.reconcileTopologyAuth(ctx, ts)
			require.NoError(t, err)
			require.True(t, ready)
			require.True(t, ts.Status.AuthEnabled)

			auth.enabled = false
			if newCluster {
				auth.clusterID++
				clear(auth.users)
				clear(auth.roles)
			}
			auth.mutations = nil
			hidden := authGrantCluster("", "hidden")
			r.APIReader = authConfirmationReader(t, r, known, hidden)
			ready, err = r.reconcileTopologyAuth(ctx, ts)
			require.ErrorContains(t, err, "projects/hidden")
			require.False(t, ready)
			require.False(t, auth.enabled)
			require.True(t, ts.Status.AuthEnabled)
			require.Empty(t, auth.mutations)

			hidden = authGrantCluster("hidden", "hidden")
			r.APIReader = authConfirmationReader(t, r, known, hidden)
			ready, err = r.reconcileTopologyAuth(ctx, ts)
			require.NoError(t, err)
			require.True(t, ready)
			require.True(t, auth.enabled)
			require.Contains(t, auth.roles, "/multigres/hidden")
			require.Contains(t, ts.Status.AuthIdentities, "/multigres/hidden")
		})
	}
}

func TestSharedSourceGrantRequiresTrackedOperatorCredential(t *testing.T) {
	for name, tc := range map[string]struct {
		contributor     string
		customCellCert  bool
		cellLocal       bool
		sameDestination bool
		wantRefusal     bool
	}{
		"tracked source":            {contributor: "projects/cluster"},
		"tracked local source":      {contributor: "projects/cluster", cellLocal: true},
		"untracked local source":    {contributor: "projects/other", cellLocal: true, wantRefusal: true},
		"different contributor":     {contributor: "projects/other", wantRefusal: true},
		"custom Cell credential":    {contributor: "projects/cluster", customCellCert: true, wantRefusal: true},
		"source equals destination": {contributor: "projects/cluster", sameDestination: true, wantRefusal: true},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := authGrantCluster("project", "cluster")
			const identity = "/multigres/project"
			const endpoint = "shared.stores:2379"
			const root = identity + "/global"
			global := cluster.Spec.GlobalTopoServer.External
			global.CASecret, global.ClientCertSecret = "destination-ca", "destination-client"
			if !tc.sameDestination {
				global.Endpoints = []api.EndpointUrl{"other.stores:2379"}
			}
			secret := api.TopoClientCertSecretName(cluster.Name)
			cellSecret := secret
			if tc.customCellCert {
				cellSecret = "source-custom-client"
			}
			cell := &api.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name: "zone", Namespace: cluster.Namespace,
					OwnerReferences: controllerRef("MultigresCluster", cluster),
				},
				Spec: api.CellSpec{
					Name: "zone",
					GlobalTopoServer: api.GlobalTopoServerRef{
						Address:          endpoint,
						RootPath:         root,
						CASecret:         secret,
						ClientCertSecret: cellSecret,
					},
				},
			}
			if tc.cellLocal {
				cell.Spec.GlobalTopoServer.Address = "previous-global.stores:2379"
				cell.Spec.TopoServer = &api.LocalTopoServerSpec{
					External: &api.ExternalTopoServerSpec{
						Endpoints: []api.EndpointUrl{endpoint},
						RootPath:  root,
					},
				}
			}
			ts := &api.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
				Status: api.TopoServerStatus{
					AuthEnabled: true, AuthIdentities: []string{identity},
					AuthIdentityClusters: map[string]string{identity: tc.contributor},
				},
			}
			grants, refused, err := desiredEtcdGrants(
				t.Context(), authGrantReconciler(t, cluster, cell).Client, ts,
			)
			require.NoError(t, err)
			if tc.wantRefusal {
				requireConfigRefusal(t, refused, cluster,
					"operator-issued topology client certificate is required")
				require.Empty(t, grants)
				return
			}
			require.Empty(t, refused)
			require.Equal(t, []etcdIdentityGrant{{
				Name: identity, Permissions: []etcdPermission{etcdPrefixPermission(root + "/")},
				Clusters: []client.ObjectKey{client.ObjectKeyFromObject(cluster)},
			}}, grants)
		})
	}
}

func TestDesiredEtcdGrantsUsesMaterializedCellCredentialDuringGlobalMove(t *testing.T) {
	for name, tc := range map[string]struct {
		certName    string
		wantRefusal bool
	}{
		"operator issued": {certName: api.TopoClientCertSecretName("owner")},
		"custom":          {certName: "old-custom-cert", wantRefusal: true},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := authGrantCluster("project", "owner")
			global := cluster.Spec.GlobalTopoServer.External
			global.Endpoints = []api.EndpointUrl{"other.stores:2379"}
			global.CASecret = "destination-ca"
			global.ClientCertSecret = "destination-client"
			ts := &api.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name: "managed", Namespace: cluster.Namespace,
					OwnerReferences: controllerRef("MultigresCluster", cluster),
				},
				Spec: api.TopoServerSpec{
					Etcd: &api.EtcdSpec{RootPath: "/multigres/project/global"},
				},
				Status: api.TopoServerStatus{AuthEnabled: true},
			}
			cell := &api.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name: "zone", Namespace: cluster.Namespace,
					Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
				},
				Spec: api.CellSpec{
					Name: "zone",
					GlobalTopoServer: api.GlobalTopoServerRef{
						Address: "managed.projects:2379", RootPath: ts.Spec.Etcd.RootPath,
						CASecret: tc.certName, ClientCertSecret: tc.certName,
					},
				},
			}

			grants, refused, err := desiredEtcdGrants(
				t.Context(), authGrantReconciler(t, cluster, cell).Client, ts,
			)
			require.NoError(t, err)
			if tc.wantRefusal {
				requireConfigRefusal(t, refused, cluster,
					"operator-issued topology client certificate is required")
				require.Empty(t, grants)
				return
			}
			require.Empty(t, refused)
			require.Len(t, grants, 1)
			require.Equal(
				t,
				[]etcdPermission{etcdPrefixPermission(ts.Spec.Etcd.RootPath + "/")},
				grants[0].Permissions,
			)
		})
	}
}

func TestDesiredEtcdGrantsRejectsUnknownOwnerKind(t *testing.T) {
	ts := &api.TopoServer{ObjectMeta: metav1.ObjectMeta{
		Name: "managed", Namespace: "projects",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "example.com/v1",
			Kind:       "UnexpectedOwner",
			Name:       "owner",
			UID:        "owner-uid",
			Controller: ptr.To(true),
		}},
	}}

	reader := authGrantReconciler(t).Client
	grants, _, err := desiredEtcdGrants(t.Context(), reader, ts)
	require.ErrorContains(t, err, `unsupported topology server owner kind "UnexpectedOwner"`)
	require.Nil(t, grants)
}

func TestCellTopologyServers(t *testing.T) {
	external := func(endpoints ...api.EndpointUrl) *api.LocalTopoServerSpec {
		return &api.LocalTopoServerSpec{External: &api.ExternalTopoServerSpec{Endpoints: endpoints}}
	}
	for name, tc := range map[string]struct {
		global string
		local  *api.LocalTopoServerSpec
		want   []client.ObjectKey
	}{
		"bare service name resolves in the cell namespace": {
			global: "shared:2379",
			want:   []client.ObjectKey{{Namespace: "projects", Name: "shared"}},
		},
		"FQDN in another namespace": {
			global: "https://shared.stores.svc.cluster.local:2379",
			want:   []client.ObjectKey{{Namespace: "stores", Name: "shared"}},
		},
		"headless pod name": {
			global: "https://shared-0.shared-headless.stores.svc.cluster.local:2379",
			want: []client.ObjectKey{
				{Namespace: "stores", Name: "shared-headless"},
				{Namespace: "stores", Name: "shared"},
			},
		},
		"external local endpoints": {
			global: "global.stores:2379",
			local:  external("https://local-a.stores:2379", "https://local-b.other.svc:2379"),
			want: []client.ObjectKey{
				{Namespace: "stores", Name: "global"},
				{Namespace: "stores", Name: "local-a"},
				{Namespace: "other", Name: "local-b"},
			},
		},
		"managed local etcd adds no key": {
			global: "shared.stores:2379",
			local:  &api.LocalTopoServerSpec{Etcd: &api.EtcdSpec{}},
			want:   []client.ObjectKey{{Namespace: "stores", Name: "shared"}},
		},
		"IP address":    {global: "10.0.0.1:2379"},
		"non-etcd port": {global: "https://shared.stores:2380"},
		"duplicates collapse": {
			global: "https://shared.stores:2379",
			local: external(
				"https://shared.stores.svc:2379",
				"https://shared-1.shared-headless.stores.svc.cluster.local:2379",
			),
			want: []client.ObjectKey{
				{Namespace: "stores", Name: "shared"},
				{Namespace: "stores", Name: "shared-headless"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cell := &api.Cell{
				ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "projects"},
				Spec: api.CellSpec{
					GlobalTopoServer: api.GlobalTopoServerRef{Address: tc.global},
					TopoServer:       tc.local,
				},
			}
			require.Equal(t, tc.want, cellTopologyServers(cell))

			keys := make([]string, 0, len(tc.want))
			for _, key := range tc.want {
				keys = append(keys, key.String())
			}
			require.Equal(t, keys, cellTopologyServerKeys(cell))
		})
	}
}

func TestHeadlessSuffixedServerNames(t *testing.T) {
	for name, tc := range map[string]struct {
		server   string
		endpoint string
	}{
		"server named shared-headless by its client Service": {
			server: "shared-headless", endpoint: "https://shared-headless.stores:2379",
		},
		"server named shared-headless by its pod": {
			server:   "shared-headless",
			endpoint: "https://shared-headless-0.shared-headless-headless.stores.svc:2379",
		},
		"server named shared by its headless Service": {
			server: "shared", endpoint: "https://shared-headless.stores:2379",
		},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := authGrantCluster("proj_123", "one")
			roots, err := topology.ForCluster(cluster)
			require.NoError(t, err)
			root, err := roots.Cell("a")
			require.NoError(t, err)
			cell := &api.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name: "a", Namespace: cluster.Namespace,
					Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
				},
				Spec: api.CellSpec{
					Name: "a",
					GlobalTopoServer: api.GlobalTopoServerRef{
						Address: tc.endpoint, RootPath: roots.Global(),
					},
					TopoServer: &api.LocalTopoServerSpec{External: &api.ExternalTopoServerSpec{
						Endpoints: []api.EndpointUrl{api.EndpointUrl(tc.endpoint)},
						RootPath:  root,
					}},
				},
			}
			ts := &api.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: tc.server, Namespace: "stores"},
				Spec:       api.TopoServerSpec{Auth: &api.TopoServerAuthSpec{Enabled: true}},
			}
			r := authGrantReconciler(t, cluster, cell, ts)

			grants, _, err := desiredEtcdGrants(context.Background(), r.Client, ts)
			require.NoError(t, err)
			require.Len(t, grants, 1)
			require.Equal(t, roots.ClusterRoot(), grants[0].Name)

			require.Equal(
				t,
				[]reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(ts)}},
				r.authManagedTopoServersForCell(context.Background(), cell),
			)
		})
	}
}

func TestAuthManagedTopoServersForCell(t *testing.T) {
	server := func(name string) *api.TopoServer {
		return &api.TopoServer{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "stores"}}
	}
	requested := server("requested")
	requested.Spec.Auth = &api.TopoServerAuthSpec{Enabled: true}
	activated := server("activated")
	activated.Status.AuthEnabled = true
	cell := &api.Cell{
		ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "projects"},
		Spec: api.CellSpec{
			GlobalTopoServer: api.GlobalTopoServerRef{Address: "https://requested.stores:2379"},
			TopoServer: &api.LocalTopoServerSpec{External: &api.ExternalTopoServerSpec{
				Endpoints: []api.EndpointUrl{
					"https://activated.stores:2379",
					"https://plain.stores:2379",
					"https://missing.stores:2379",
				},
			}},
		},
	}
	request := func(name string) reconcile.Request {
		return reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "stores", Name: name},
		}
	}

	r := authGrantReconciler(t, requested, activated, server("plain"))
	require.Equal(
		t,
		[]reconcile.Request{request("requested"), request("activated")},
		r.authManagedTopoServersForCell(context.Background(), cell),
	)
	require.Empty(t, r.authManagedTopoServersForCell(context.Background(), requested))

	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		Get: func(
			context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption,
		) error {
			return errors.New("cache unavailable")
		},
	})
	require.Equal(
		t,
		[]reconcile.Request{
			request("requested"), request("activated"), request("plain"), request("missing"),
		},
		r.authManagedTopoServersForCell(context.Background(), cell),
		"an unreadable TopoServer is enqueued rather than skipped",
	)
}
