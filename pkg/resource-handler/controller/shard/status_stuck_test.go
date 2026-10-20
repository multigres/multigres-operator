package shard

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func poolPodsStuckMetric(t *testing.T, shardName, pool, cell string) (float64, bool) {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "multigres_operator_pool_pods_stuck" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["shard"] == shardName && labels["pool"] == pool && labels["cell"] == cell {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func TestUpdateStatus_PoolPodsStuckMetric(t *testing.T) {
	zoneNode := func(name string, ready bool) *corev1.Node {
		n := stuckTestNode("zone-a", "customer-pg", ready, false)
		n.Name = name
		return &n
	}

	tests := []struct {
		name  string
		nodes []client.Object
		want  float64
	}{
		{
			name:  "zone down: pending pod does not count",
			nodes: []client.Object{zoneNode("node-1", false)},
			want:  0,
		},
		{
			name:  "zone healthy: pending pod counts",
			nodes: []client.Object{zoneNode("node-1", true)},
			want:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = multigresv1alpha1.AddToScheme(scheme)
			_ = appsv1.AddToScheme(scheme)
			_ = corev1.AddToScheme(scheme)

			shard := &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "stuck-shard", Namespace: "default"},
				Spec: multigresv1alpha1.ShardSpec{
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"pool1": {Cells: []multigresv1alpha1.CellName{"zone-a"}},
					},
				},
			}
			selector := metadata.GetSelectorLabels(
				buildPoolLabelsWithCell(shard, "pool1", "zone-a"),
			)
			pod := stuckTestPod("zone-a", false)
			pod.Name = "pool1-0"
			pod.Namespace = "default"
			pod.Labels = selector

			objs := append([]client.Object{shard, &pod}, tt.nodes...)
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				WithStatusSubresource(&multigresv1alpha1.Shard{}).
				Build()
			reconciler := &ShardReconciler{
				Client:    fakeClient,
				Scheme:    scheme,
				Recorder:  record.NewFakeRecorder(100),
				APIReader: fakeClient,
			}
			t.Cleanup(func() { monitoring.ForgetShardPoolPodsStuck("stuck-shard", "default") })

			if err := reconciler.updateStatus(
				context.Background(),
				shard,
				renderedConfig{},
			); err != nil {
				t.Fatalf("updateStatus() error: %v", err)
			}

			got, ok := poolPodsStuckMetric(t, "stuck-shard", "pool1", "zone-a")
			if !ok {
				t.Fatal("pool_pods_stuck series not published")
			}
			if got != tt.want {
				t.Errorf("pool_pods_stuck = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcile_NotFoundForgetsPoolPodsStuck(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	samples := []monitoring.PoolPodsStuck{{Pool: "pool1", Cell: "zone-a", Count: 3}}
	monitoring.ReplaceShardPoolPodsStuck("c", "deleted-shard", "default", samples)
	monitoring.ReplaceShardPoolPodsStuck("c", "other-shard", "default", samples)
	t.Cleanup(func() {
		monitoring.ForgetShardPoolPodsStuck("deleted-shard", "default")
		monitoring.ForgetShardPoolPodsStuck("other-shard", "default")
	})

	reconciler := &ShardReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "deleted-shard", Namespace: "default"},
	}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}

	if _, ok := poolPodsStuckMetric(t, "deleted-shard", "pool1", "zone-a"); ok {
		t.Error("series for deleted shard must be removed")
	}
	if got, ok := poolPodsStuckMetric(t, "other-shard", "pool1", "zone-a"); !ok || got != 3 {
		t.Errorf("other shard series = %v (present=%v), want 3 present", got, ok)
	}
}

func TestUpdateStatus_NodeListFailureKeepsPoolPodsStuck(t *testing.T) {
	tests := []struct {
		name         string
		failNodeList bool
		want         float64
	}{
		{name: "node list fails: previous value kept", failNodeList: true, want: 5},
		{name: "node list succeeds: value refreshed", failNodeList: false, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = multigresv1alpha1.AddToScheme(scheme)
			_ = appsv1.AddToScheme(scheme)
			_ = corev1.AddToScheme(scheme)

			shard := &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "stuck-shard", Namespace: "default"},
				Spec: multigresv1alpha1.ShardSpec{
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"pool1": {Cells: []multigresv1alpha1.CellName{"zone-a", "zone-b"}},
					},
				},
			}
			pods := []client.Object{}
			for _, cell := range []string{"zone-a", "zone-b"} {
				pod := stuckTestPod(cell, false)
				pod.Name = "pool1-" + cell
				pod.Namespace = "default"
				pod.Labels = metadata.GetSelectorLabels(
					buildPoolLabelsWithCell(shard, "pool1", cell),
				)
				pods = append(pods, &pod)
			}
			node := stuckTestNode("zone-a", "customer-pg", true, false)
			node.Name = "node-1"
			nodeB := stuckTestNode("zone-b", "customer-pg", true, false)
			nodeB.Name = "node-2"

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(append(pods, shard, &node, &nodeB)...).
				WithStatusSubresource(&multigresv1alpha1.Shard{}).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(
						ctx context.Context,
						c client.WithWatch,
						list client.ObjectList,
						opts ...client.ListOption,
					) error {
						if _, ok := list.(*corev1.NodeList); ok && tt.failNodeList {
							return errors.New("nodes unavailable")
						}
						return c.List(ctx, list, opts...)
					},
				}).
				Build()
			reconciler := &ShardReconciler{
				Client:    fakeClient,
				Scheme:    scheme,
				Recorder:  record.NewFakeRecorder(100),
				APIReader: fakeClient,
			}
			t.Cleanup(func() { monitoring.ForgetShardPoolPodsStuck("stuck-shard", "default") })

			monitoring.ReplaceShardPoolPodsStuck(
				"c",
				"stuck-shard",
				"default",
				[]monitoring.PoolPodsStuck{
					{Pool: "pool1", Cell: "zone-a", Count: 5},
					{Pool: "pool1", Cell: "zone-b", Count: 5},
				},
			)

			if err := reconciler.updateStatus(
				context.Background(),
				shard,
				renderedConfig{},
			); err != nil {
				t.Fatalf("updateStatus() error: %v", err)
			}

			for _, cell := range []string{"zone-a", "zone-b"} {
				got, ok := poolPodsStuckMetric(t, "stuck-shard", "pool1", cell)
				if !ok {
					t.Fatalf("pool_pods_stuck series for %s missing", cell)
				}
				if got != tt.want {
					t.Errorf("%s: pool_pods_stuck = %v, want %v", cell, got, tt.want)
				}
			}
		})
	}
}

func TestReconcile_DeletionPathsForgetPoolPodsStuck(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	now := metav1.Now()
	tests := []struct {
		name string
		meta metav1.ObjectMeta
	}{
		{
			name: "deletion timestamp",
			meta: metav1.ObjectMeta{
				Name:              "draining-shard",
				Namespace:         "default",
				DeletionTimestamp: &now,
				Finalizers:        []string{shardFinalizer},
			},
		},
		{
			name: "pending deletion annotation",
			meta: metav1.ObjectMeta{
				Name:       "draining-shard",
				Namespace:  "default",
				Finalizers: []string{shardFinalizer},
				Annotations: map[string]string{
					multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shard := &multigresv1alpha1.Shard{
				ObjectMeta: tt.meta,
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "shard-0",
				},
			}
			samples := []monitoring.PoolPodsStuck{{Pool: "pool1", Cell: "zone-a", Count: 3}}
			monitoring.ReplaceShardPoolPodsStuck("c", "draining-shard", "default", samples)
			monitoring.ReplaceShardPoolPodsStuck("c", "other-shard", "default", samples)
			t.Cleanup(func() {
				monitoring.ForgetShardPoolPodsStuck("draining-shard", "default")
				monitoring.ForgetShardPoolPodsStuck("other-shard", "default")
			})

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(shard).
				WithStatusSubresource(&multigresv1alpha1.Shard{}).
				Build()
			reconciler := &ShardReconciler{
				Client:          c,
				Scheme:          scheme,
				Recorder:        record.NewFakeRecorder(10),
				CreateTopoStore: newMemoryTopoFactory(),
			}
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{Name: "draining-shard", Namespace: "default"},
			}
			if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
				t.Fatalf("Reconcile() error: %v", err)
			}

			if _, ok := poolPodsStuckMetric(t, "draining-shard", "pool1", "zone-a"); ok {
				t.Error("series for deleting shard must be removed")
			}
			if got, ok := poolPodsStuckMetric(
				t,
				"other-shard",
				"pool1",
				"zone-a",
			); !ok ||
				got != 3 {
				t.Errorf("other shard series = %v (present=%v), want 3 present", got, ok)
			}
		})
	}
}
