package multigrescluster

import (
	"context"
	"path"
	"reflect"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConvergedTopologyReconcileDoesNotWrite(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://topo:2379"},
				},
			},
			Cells: []multigresv1alpha1.CellConfig{
				{Name: "cell1"},
			},
			Databases: []multigresv1alpha1.DatabaseConfig{{Name: "db"}},
		},
	}
	store := newClusterTopologyMemoryStore(t)
	c := newTestClient(t, cluster)
	r := &MultigresClusterReconciler{
		Client:          c,
		Scheme:          setupScheme(),
		Recorder:        record.NewFakeRecorder(100),
		CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) { return noCloseStore{store}, nil },
	}
	res := resolver.NewResolver(c, cluster.Namespace)
	if _, err := r.reconcileTopology(ctx, cluster, res); err != nil {
		t.Fatal(err)
	}
	conn, err := store.ConnForCell(ctx, topoclient.GlobalCell)
	if err != nil {
		t.Fatal(err)
	}
	versions := map[string]string{}
	for _, file := range []string{path.Join(topoclient.CellsPath, "cell1", topoclient.CellFile), path.Join(topoclient.DatabasesPath, "db", topoclient.DatabaseFile)} {
		_, v, err := conn.Get(ctx, file)
		if err != nil {
			t.Fatal(err)
		}
		versions[file] = v.String()
	}
	for range 5 {
		if _, err := r.reconcileTopology(ctx, cluster, res); err != nil {
			t.Fatal(err)
		}
		for file, want := range versions {
			_, v, err := conn.Get(ctx, file)
			if err != nil {
				t.Fatal(err)
			}
			if v.String() != want {
				t.Fatalf("converged reconcile rewrote %s", file)
			}
		}
	}
}

func TestReconcileTopologySharedTopo(t *testing.T) {
	t.Parallel()

	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://global-etcd:2379"},
					RootPath:  "/multigres/clusters/cluster/global",
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{
				Name:   "cell1",
				ZoneID: "use1-az1",
				Spec: &multigresv1alpha1.CellInlineSpec{
					LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						External: &multigresv1alpha1.ExternalTopoServerSpec{
							Endpoints: []multigresv1alpha1.EndpointUrl{
								"http://cell1-local-etcd:2379",
							},
							RootPath: "/multigres/clusters/cluster/cells/cell1",
						},
					},
				},
			}},
			Databases: []multigresv1alpha1.DatabaseConfig{{Name: "commerce"}},
		},
	}

	store := newClusterTopologyMemoryStore(t)
	var openedRef multigresv1alpha1.GlobalTopoServerRef
	kc := newTestClient(t, cluster)
	reconciler := &MultigresClusterReconciler{
		Client:   kc,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(ref multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			openedRef = ref
			return noCloseStore{Store: store}, nil
		},
	}

	result, err := reconciler.reconcileTopology(
		context.Background(),
		cluster,
		resolver.NewResolver(kc, "default"),
	)
	if err != nil {
		t.Fatalf("reconcileTopology() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0", result.RequeueAfter)
	}

	if openedRef.Address != "http://global-etcd:2379" {
		t.Fatalf("expected topology store to open global address, got %q", openedRef.Address)
	}
	if openedRef.RootPath != "/multigres/clusters/cluster/global" {
		t.Fatalf("expected topology store to open global root, got %q", openedRef.RootPath)
	}

	cell, err := store.GetCell(context.Background(), "cell1")
	if err != nil {
		t.Fatalf("cell not found: %v", err)
	}
	if !reflect.DeepEqual(cell.ServerAddresses, []string{"http://cell1-local-etcd:2379"}) {
		t.Errorf("expected cell record to point at local topology, got %v", cell.ServerAddresses)
	}
	if cell.Root != "/multigres/clusters/cluster/cells/cell1" {
		t.Errorf("expected cell record local root, got %q", cell.Root)
	}

	db, err := store.GetDatabase(context.Background(), "commerce")
	if err != nil {
		t.Fatalf("database not found in global topology store: %v", err)
	}
	if db.Name != "commerce" {
		t.Errorf("expected database commerce, got %q", db.Name)
	}
}

func TestReconcileTopologyManagedLocalTopo(t *testing.T) {
	t.Parallel()

	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://global-etcd:2379"},
					RootPath:  "/multigres/global",
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{
				Name:   "cell1",
				ZoneID: "use1-az1",
				Spec: &multigresv1alpha1.CellInlineSpec{
					LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
				},
			}},
		},
	}

	store := newClusterTopologyMemoryStore(t)
	k8sCell := expectedManagedLocalCell("cluster", "cell1", "default")
	ts := healthyManagedLocalTopoServer(k8sCell)
	kc := newTestClient(t, cluster, k8sCell, ts)
	ts.Status.ObservedGeneration = ts.Generation
	if err := kc.Status().Update(context.Background(), ts); err != nil {
		t.Fatalf("failed to update TopoServer status: %v", err)
	}
	reconciler := &MultigresClusterReconciler{
		Client:   kc,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(ref multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return noCloseStore{Store: store}, nil
		},
	}
	result, err := reconciler.reconcileTopology(
		context.Background(),
		cluster,
		resolver.NewResolver(kc, "default"),
	)
	if err != nil {
		t.Fatalf("reconcileTopology() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0", result.RequeueAfter)
	}

	cell, err := store.GetCell(context.Background(), "cell1")
	if err != nil {
		t.Fatalf("cell not found: %v", err)
	}
	wantAddress := topo.ManagedLocalTopoServerAddress(
		name.JoinWithConstraints(name.DefaultConstraints, "cluster", "cell1"),
		"default",
	)
	if !reflect.DeepEqual(cell.ServerAddresses, []string{wantAddress}) {
		t.Errorf("expected managed local topology service, got %v", cell.ServerAddresses)
	}
	if cell.Root != "/multigres/default/cluster/cell1" {
		t.Errorf("expected default local root, got %q", cell.Root)
	}
}

func TestReconcileTopologyWaitsForManagedLocalTopoNotOwnedByCell(t *testing.T) {
	t.Parallel()

	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://global-etcd:2379"},
					RootPath:  "/multigres/global",
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{
				Name: "cell1",
				Spec: &multigresv1alpha1.CellInlineSpec{
					LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
				},
			}},
		},
	}

	cell := expectedManagedLocalCell("cluster", "cell1", "default")
	ts := healthyManagedLocalTopoServer(cell)
	ts.OwnerReferences = nil
	kc := newTestClient(t, cluster, cell, ts)
	ts.Status.ObservedGeneration = ts.Generation
	if err := kc.Status().Update(context.Background(), ts); err != nil {
		t.Fatalf("failed to update TopoServer status: %v", err)
	}
	reconciler := &MultigresClusterReconciler{
		Client:   kc,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(ref multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			t.Fatal("topology store should not be opened for an unowned local TopoServer")
			return nil, nil
		},
	}

	result, err := reconciler.reconcileTopology(
		context.Background(),
		cluster,
		resolver.NewResolver(kc, "default"),
	)
	if err != nil {
		t.Fatalf("reconcileTopology() error = %v", err)
	}
	if result.RequeueAfter != localTopoServerRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, localTopoServerRequeueDelay)
	}
}

func TestReconcileTopologyWaitsForManagedLocalTopo(t *testing.T) {
	t.Parallel()

	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://global-etcd:2379"},
					RootPath:  "/multigres/global",
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{
				Name: "cell1",
				Spec: &multigresv1alpha1.CellInlineSpec{
					LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
				},
			}},
		},
	}

	store := newClusterTopologyMemoryStore(t)
	kc := newTestClient(t, cluster)
	reconciler := &MultigresClusterReconciler{
		Client:   kc,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(ref multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return noCloseStore{Store: store}, nil
		},
	}
	result, err := reconciler.reconcileTopology(
		context.Background(),
		cluster,
		resolver.NewResolver(kc, "default"),
	)
	if err != nil {
		t.Fatalf("reconcileTopology() error = %v", err)
	}
	if result.RequeueAfter != localTopoServerRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, localTopoServerRequeueDelay)
	}
	if _, err := store.GetCell(context.Background(), "cell1"); err == nil {
		t.Fatal("cell should not be registered before managed local TopoServer is ready")
	}
}

func TestReconcileTopologyKeepsExistingCellRecordWhileManagedLocalTopoWaits(t *testing.T) {
	t.Parallel()

	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://global-etcd:2379"},
					RootPath:  "/multigres/global",
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{
				Name: "cell1",
				Spec: &multigresv1alpha1.CellInlineSpec{
					LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
				},
			}},
		},
	}

	store := newClusterTopologyMemoryStore(t)
	if err := topo.RegisterCellFromSpec(
		context.Background(),
		store,
		record.NewFakeRecorder(10),
		cluster,
		multigresv1alpha1.CellConfig{Name: "cell1"},
		nil,
		multigresv1alpha1.GlobalTopoServerRef{
			Address:  "http://global-etcd:2379",
			RootPath: "/multigres/global",
		},
	); err != nil {
		t.Fatalf("failed to seed existing cell topology: %v", err)
	}

	kc := newTestClient(t, cluster)
	reconciler := &MultigresClusterReconciler{
		Client:   kc,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(ref multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return noCloseStore{Store: store}, nil
		},
	}

	result, err := reconciler.reconcileTopology(
		context.Background(),
		cluster,
		resolver.NewResolver(kc, "default"),
	)
	if err != nil {
		t.Fatalf("reconcileTopology() error = %v", err)
	}
	if result.RequeueAfter != localTopoServerRequeueDelay {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, localTopoServerRequeueDelay)
	}

	cell, err := store.GetCell(context.Background(), "cell1")
	if err != nil {
		t.Fatalf("existing cell record should remain available: %v", err)
	}
	if !reflect.DeepEqual(cell.ServerAddresses, []string{"http://global-etcd:2379"}) {
		t.Errorf("existing cell address = %v, want global topology address", cell.ServerAddresses)
	}
	if cell.Root != "/multigres/global" {
		t.Errorf("existing cell root = %q, want global topology root", cell.Root)
	}
}

func TestReconcileTopologyKeepsPendingDeletionCellRecord(t *testing.T) {
	t.Parallel()

	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://global-etcd:2379"},
					RootPath:  "/multigres/global",
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "cell1"}},
		},
	}
	pendingCell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-cell2",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "cluster",
			},
			Annotations: map[string]string{
				multigresv1alpha1.AnnotationPendingDeletion: "2026-05-21T00:00:00Z",
			},
		},
		Spec: multigresv1alpha1.CellSpec{Name: "cell2"},
	}

	store := newClusterTopologyMemoryStore(t)
	for _, cellName := range []multigresv1alpha1.CellName{"cell1", "cell2"} {
		if err := topo.RegisterCellFromSpec(
			context.Background(),
			store,
			record.NewFakeRecorder(10),
			cluster,
			multigresv1alpha1.CellConfig{Name: cellName},
			nil,
			multigresv1alpha1.GlobalTopoServerRef{
				Address:  "http://global-etcd:2379",
				RootPath: "/multigres/global",
			},
		); err != nil {
			t.Fatalf("failed to seed cell %s topology: %v", cellName, err)
		}
	}

	kc := newTestClient(t, cluster, pendingCell)
	reconciler := &MultigresClusterReconciler{
		Client:   kc,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(ref multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return noCloseStore{Store: store}, nil
		},
	}

	result, err := reconciler.reconcileTopology(
		context.Background(),
		cluster,
		resolver.NewResolver(kc, "default"),
		true,
	)
	if err != nil {
		t.Fatalf("reconcileTopology() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0", result.RequeueAfter)
	}

	if _, err := store.GetCell(context.Background(), "cell1"); err != nil {
		t.Fatalf("active cell record should remain: %v", err)
	}
	if _, err := store.GetCell(context.Background(), "cell2"); err != nil {
		t.Fatalf("pending-deletion cell record should remain until deletion completes: %v", err)
	}
}

func newClusterTopologyMemoryStore(t *testing.T) topoclient.Store {
	t.Helper()
	_, factory := memorytopo.NewServerAndFactory(context.Background())
	store := topoclient.NewWithFactory(
		factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
	)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

type noCloseStore struct {
	topoclient.Store
}

func (s noCloseStore) Close() error {
	return nil
}

func expectedManagedLocalCell(clusterName, cellName, namespace string) *multigresv1alpha1.Cell {
	return &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name.JoinWithConstraints(name.DefaultConstraints, clusterName, cellName),
			Namespace: namespace,
			UID:       types.UID(clusterName + "-" + cellName + "-uid"),
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: multigresv1alpha1.CellName(cellName),
		},
	}
}

func healthyManagedLocalTopoServer(cell *multigresv1alpha1.Cell) *multigresv1alpha1.TopoServer {
	trueValue := true
	return &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      topo.ManagedLocalTopoServerName(cell.Name),
			Namespace: cell.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         multigresv1alpha1.GroupVersion.String(),
				Kind:               "Cell",
				Name:               cell.Name,
				UID:                cell.UID,
				Controller:         &trueValue,
				BlockOwnerDeletion: &trueValue,
			}},
		},
		Spec: multigresv1alpha1.TopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{},
		},
		Status: multigresv1alpha1.TopoServerStatus{
			ObservedGeneration: 1,
			Phase:              multigresv1alpha1.PhaseHealthy,
		},
	}
}
