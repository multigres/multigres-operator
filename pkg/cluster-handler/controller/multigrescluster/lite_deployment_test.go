package multigrescluster

import (
	"reflect"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres/go/common/topoclient"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLiteGlobalServiceSelectsOnlyItsOwnPod(t *testing.T) {
	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "lite", Namespace: "test"},
		Spec:       multigresv1alpha1.MultigresClusterSpec{Mode: multigresv1alpha1.ModeLite},
	}
	pool := multigresv1alpha1.PoolSpec{}
	sh := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lite-shard",
			Namespace: "test",
			Labels:    map[string]string{"multigres.com/cluster": "lite"},
		},
		Spec: multigresv1alpha1.ShardSpec{Mode: multigresv1alpha1.ModeLite},
	}
	pod, err := shard.BuildLitePod(sh, "default", "zone-a", pool, scheme)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := BuildMultigatewayGlobalService(cluster, nil, scheme)
	if err != nil {
		t.Fatal(err)
	}
	selector := labels.SelectorFromSet(svc.Spec.Selector)
	if !selector.Matches(labels.Set(pod.Labels)) {
		t.Fatalf("Service %v does not select Lite pod %v", svc.Spec.Selector, pod.Labels)
	}
	pod.Labels["app.kubernetes.io/instance"] = "other"
	if selector.Matches(labels.Set(pod.Labels)) {
		t.Fatal("Service crosses project identity")
	}
	if svc.Spec.Ports[0].Port != 5432 || svc.Spec.Ports[0].TargetPort.StrVal != "postgres" {
		t.Fatal("SQL port mapping changed")
	}
}

func TestLiteParentPreservesResolvedShardTopology(t *testing.T) {
	ctx := t.Context()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "lite", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Mode: multigresv1alpha1.ModeLite,
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://topo:2379"},
				},
			},
			Cells:     []multigresv1alpha1.CellConfig{{Name: "a"}, {Name: "b"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{Name: "postgres"}},
		},
	}
	store := newClusterTopologyMemoryStore(t)
	cl := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(cluster).Build()
	recorder := record.NewFakeRecorder(100)
	sh := &multigresv1alpha1.Shard{
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "postgres",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {Cells: []multigresv1alpha1.CellName{"a"}},
			},
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeS3,
				S3: &multigresv1alpha1.S3BackupConfig{
					Bucket:    "lite",
					Region:    "us-east-1",
					KeyPrefix: "resolved-shard/",
				},
			},
		},
	}
	// Seed the record the way the lite Shard registers it.
	if err := topo.RegisterDatabaseFromSpec(
		ctx, store, recorder, sh,
		multigresv1alpha1.DatabaseConfig{
			Name:             sh.Spec.DatabaseName,
			DurabilityPolicy: sh.Spec.DurabilityPolicy,
		},
		topo.CollectCells(sh), sh.Spec.Backup, sh.Spec.DurabilityPolicy,
	); err != nil {
		t.Fatal(err)
	}
	before, err := store.GetDatabase(ctx, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	r := &MultigresClusterReconciler{
		Client:          cl,
		Scheme:          setupScheme(),
		Recorder:        recorder,
		CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) { return noCloseStore{store}, nil },
	}
	for range 3 {
		if _, err := r.reconcileTopology(
			ctx,
			cluster,
			resolver.NewResolver(cl, cluster.Namespace),
		); err != nil {
			t.Fatal(err)
		}
	}
	after, err := store.GetDatabase(ctx, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("parent overwrote resolved shard database: before=%v after=%v", before, after)
	}
}
