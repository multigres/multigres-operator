package multigrescluster

import (
	"context"
	"reflect"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	reconciler := &MultigresClusterReconciler{
		Client:   client,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(ref multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			openedRef = ref
			return noCloseStore{Store: store}, nil
		},
	}

	if _, err := reconciler.reconcileTopology(
		context.Background(),
		cluster,
		resolver.NewResolver(client, "default"),
	); err != nil {
		t.Fatalf("reconcileTopology() error = %v", err)
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
