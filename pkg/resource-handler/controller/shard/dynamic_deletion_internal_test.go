package shard

import (
	"slices"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// registerPooler writes a pooler record for postgres/commerce/<shard> as a
// pooler run with --tablegroup-id=id does.
func registerPooler(t *testing.T, store topoclient.Store, name, shard string, id int64) {
	t.Helper()
	mp := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      name,
		},
		Hostname: name,
		ShardRef: &clustermetadatapb.ShardRef{
			Database:   "postgres",
			TableGroup: "commerce",
			Shard:      shard,
		},
		TablegroupId: id,
	}
	if err := store.RegisterMultipooler(t.Context(), mp, false); err != nil {
		t.Fatal(err)
	}
}

func poolerNames(t *testing.T, store topoclient.Store) []string {
	t.Helper()
	poolers, err := store.GetMultipoolersByCell(t.Context(), "cell1", nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range poolers {
		names = append(names, p.Id.Name)
	}
	slices.Sort(names)
	return names
}

func TestHandleDeletion_DynamicShardTeardownOrder(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	labels := map[string]string{
		metadata.LabelMultigresCluster:           "c",
		metadata.LabelMultigresDatabase:          "postgres",
		metadata.LabelMultigresTableGroup:        "commerce",
		metadata.LabelMultigresShard:             "0-80",
		multigresv1alpha1.LabelDynamicTableGroup: "true",
	}
	now := metav1.Now()
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "c-postgres-commerce-0-80", Namespace: "default",
			Labels:            labels,
			Annotations:       map[string]string{multigresv1alpha1.AnnotationTableGroupID: "7"},
			DeletionTimestamp: &now,
			Finalizers:        []string{shardFinalizer},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "commerce",
			ShardName:      "0-80",
		},
	}
	// The pod's own finalizer keeps it terminating after Delete.
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "pooler-0", Namespace: "default", Labels: labels, Finalizers: []string{"test/hold"},
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(shard, pod).Build()

	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	newStore := func() topoclient.Store {
		return topoclient.NewWithFactory(
			factory,
			"",
			[]string{""},
			topoclient.NewDefaultTopoConfig(),
		)
	}
	store := newStore()
	defer func() { _ = store.Close() }()
	registerPooler(t, store, "this-incarnation", "0-80", 7)
	registerPooler(t, store, "older-incarnation", "0-80", 3)
	registerPooler(t, store, "no-id", "0-80", 0)
	registerPooler(t, store, "other-shard", "80-inf", 7)

	r := &ShardReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(20),
		CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			return newStore(), nil
		},
	}
	all := []string{"no-id", "older-incarnation", "other-shard", "this-incarnation"}

	// Pass 1: the pod is still terminating, so topology is untouched.
	res, err := r.handleDeletion(t.Context(), shard.DeepCopy())
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("did not requeue while a pod was terminating")
	}
	if got := poolerNames(t, store); !slices.Equal(got, all) {
		t.Fatalf("records changed while a pod remained: %v", got)
	}

	// Pass 2: the pod is gone.
	got := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatal(err)
	}
	got.Finalizers = nil
	if err := c.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	current := &multigresv1alpha1.Shard{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(shard), current); err != nil {
		t.Fatal(err)
	}
	if _, err := r.handleDeletion(t.Context(), current); err != nil {
		t.Fatal(err)
	}

	if got, want := poolerNames(
		t,
		store,
	), []string{
		"no-id",
		"older-incarnation",
		"other-shard",
	}; !slices.Equal(
		got,
		want,
	) {
		t.Errorf("records left = %v, want %v (only id 7 of this shard deleted)", got, want)
	}
	err = c.Get(
		t.Context(),
		types.NamespacedName{Namespace: "default", Name: shard.Name},
		&multigresv1alpha1.Shard{},
	)
	if !apierrors.IsNotFound(err) {
		t.Errorf("shard finalizer not removed: %v", err)
	}
}

func TestHandleDeletion_DynamicShardTopologyErrorKeepsFinalizer(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	now := metav1.Now()
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "s", Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster:           "c",
				multigresv1alpha1.LabelDynamicTableGroup: "true",
			},
			Annotations:       map[string]string{multigresv1alpha1.AnnotationTableGroupID: "7"},
			DeletionTimestamp: &now,
			Finalizers:        []string{shardFinalizer},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "commerce",
			ShardName:      "0-80",
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
	rec := record.NewFakeRecorder(20)
	r := &ShardReconciler{
		Client: c, Scheme: scheme, Recorder: rec,
		CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			return nil, apierrors.NewServiceUnavailable("topology down")
		},
	}
	if _, err := r.handleDeletion(t.Context(), shard.DeepCopy()); err == nil {
		t.Fatal("want an error while topology is unreachable")
	}
	if len(rec.Events) == 0 || !strings.Contains(<-rec.Events, "TopologyError") {
		t.Error("no TopologyError event")
	}
	if err := c.Get(
		t.Context(),
		client.ObjectKeyFromObject(shard),
		&multigresv1alpha1.Shard{},
	); err != nil {
		t.Errorf("shard finalizer removed before topology cleanup: %v", err)
	}
}

func TestHandleDeletion_DynamicShardSkipsTopologyWhileClusterIsDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	now := metav1.Now()
	cluster := &multigresv1alpha1.MultigresCluster{ObjectMeta: metav1.ObjectMeta{
		Name: "c", Namespace: "default", DeletionTimestamp: &now, Finalizers: []string{"test/hold"},
	}}
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "s", Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster:           "c",
				multigresv1alpha1.LabelDynamicTableGroup: "true",
			},
			Annotations:       map[string]string{multigresv1alpha1.AnnotationTableGroupID: "7"},
			DeletionTimestamp: &now,
			Finalizers:        []string{shardFinalizer},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "commerce",
			ShardName:      "0-80",
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, shard).Build()
	r := &ShardReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20),
		CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			t.Error("topology opened while the cluster is being deleted")
			return nil, apierrors.NewServiceUnavailable("topology down")
		},
	}
	if _, err := r.handleDeletion(t.Context(), shard.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(
		t.Context(),
		client.ObjectKeyFromObject(shard),
		&multigresv1alpha1.Shard{},
	); !apierrors.IsNotFound(err) {
		t.Errorf("shard finalizer not removed: %v", err)
	}
}
