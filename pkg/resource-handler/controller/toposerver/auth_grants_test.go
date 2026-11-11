package toposerver

import (
	"context"
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

	api "github.com/multigres/multigres-operator/api/v1alpha1"
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
	grants, err := desiredEtcdGrants(context.Background(), r.Client, &api.TopoServer{
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

func TestDesiredEtcdGrantsProjectIdentity(t *testing.T) {
	cluster := authGrantCluster("project", "one")
	r := authGrantReconciler(t, cluster)
	ts := &api.TopoServer{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"}}
	grants, err := desiredEtcdGrants(context.Background(), r.Client, ts)
	require.NoError(t, err)
	require.Len(t, grants, 1)
	require.Equal(t, "/multigres/project", grants[0].Name)
	require.Equal(t, []etcdPermission{{
		Key:      "/multigres/project/global/",
		RangeEnd: clientv3.GetPrefixRangeEnd("/multigres/project/global/"),
		Type:     clientv3.PermReadWrite,
	}}, grants[0].Permissions)
}

func TestDesiredEtcdGrantsIncludesCellRoot(t *testing.T) {
	cluster := authGrantCluster("project", "one")
	cluster.Spec.Cells = []api.CellConfig{{
		Name: "zone",
		Spec: &api.CellInlineSpec{LocalTopoServer: &api.LocalTopoServerSpec{
			External: &api.ExternalTopoServerSpec{
				Endpoints: []api.EndpointUrl{"https://shared.stores:2379"},
				RootPath:  "/multigres/project/zone",
			},
		}},
	}}
	grants, err := desiredEtcdGrants(
		t.Context(),
		authGrantReconciler(t, cluster).Client,
		&api.TopoServer{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"}},
	)
	require.NoError(t, err)
	require.Len(t, grants, 1)
	require.Equal(t, "/multigres/project", grants[0].Name)
	require.Equal(t, []etcdPermission{
		{Key: "/multigres/project/global/", RangeEnd: clientv3.GetPrefixRangeEnd("/multigres/project/global/"), Type: clientv3.PermReadWrite},
		{Key: "/multigres/project/zone/", RangeEnd: clientv3.GetPrefixRangeEnd("/multigres/project/zone/"), Type: clientv3.PermReadWrite},
	}, grants[0].Permissions)
}

func TestDesiredEtcdGrantsRejectInvalidReferencedRoot(t *testing.T) {
	cluster := authGrantCluster("one", "one")
	cluster.Spec.GlobalTopoServer.External.RootPath = "/multigres/other/global"
	r := authGrantReconciler(t, cluster)
	grants, err := desiredEtcdGrants(
		context.Background(),
		r.Client,
		&api.TopoServer{ObjectMeta: metav1.ObjectMeta{
			Name: "shared", Namespace: "stores",
		}},
	)
	require.ErrorContains(t, err, "outside project prefix")
	require.Nil(t, grants)
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
			grants, err := desiredEtcdGrants(
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

func TestDesiredEtcdGrantsSharedProjectAndColocatedRoots(t *testing.T) {
	first, second := authGrantCluster("project", "one"), authGrantCluster("project", "two")
	first.Spec.Cells = []api.CellConfig{{
		Name: "zone",
		Spec: &api.CellInlineSpec{
			LocalTopoServer: &api.LocalTopoServerSpec{
				External: &api.ExternalTopoServerSpec{
					Endpoints: []api.EndpointUrl{"https://shared.stores:2379"},
					RootPath:  "/multigres/project/global",
				},
			},
		},
	}}
	r := authGrantReconciler(t, first, second)
	grants, err := desiredEtcdGrants(
		context.Background(),
		r.Client,
		&api.TopoServer{ObjectMeta: metav1.ObjectMeta{
			Name: "shared", Namespace: "stores",
		}},
	)
	require.NoError(t, err)
	require.Equal(t, []etcdIdentityGrant{{
		Name: "/multigres/project",
		Permissions: []etcdPermission{{
			Key:      "/multigres/project/global/",
			RangeEnd: clientv3.GetPrefixRangeEnd("/multigres/project/global/"),
			Type:     clientv3.PermReadWrite,
		}},
	}}, grants)
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
			before, err := desiredEtcdGrants(t.Context(), r.Client, ts)
			require.NoError(t, err)
			require.Len(t, before, 1)
			require.NoError(t, r.Delete(t.Context(), terminating))
			after, err := desiredEtcdGrants(t.Context(), r.Client, ts)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.NoError(
				t,
				r.Get(t.Context(), client.ObjectKeyFromObject(terminating), terminating),
			)
			terminating.SetFinalizers(nil)
			require.NoError(t, r.Update(t.Context(), terminating))
			after, err = desiredEtcdGrants(t.Context(), r.Client, ts)
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
			grants, err := desiredEtcdGrants(t.Context(), r.Client, ts)
			require.ErrorContains(t, err, "cannot resolve project identity")
			require.Nil(t, grants)
		})
	}
}
