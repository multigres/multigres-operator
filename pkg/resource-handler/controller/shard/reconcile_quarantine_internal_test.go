package shard

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// markBackupHealthy sets the shard's backup-health condition to True so
// quarantine remediation's backup-health gate is satisfied.
func markBackupHealthy(shard *multigresv1alpha1.Shard) {
	shard.Status.Conditions = []metav1.Condition{{
		Type:               backuphealth.ConditionHealthy,
		Status:             metav1.ConditionTrue,
		Reason:             "Healthy",
		LastTransitionTime: metav1.Now(),
	}}
}

const (
	qrCell   = "cell1"
	qrPool   = "primary"
	qrNS     = "default"
	qrReason = "postgres failed to recover for 5m0s across 60 attempts"
)

func qrShard() *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: qrNS,
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "shard0",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				multigresv1alpha1.PoolName(qrPool): {Cells: []multigresv1alpha1.CellName{qrCell}},
			},
		},
	}
}

func qrPod(shard *multigresv1alpha1.Shard, idx int, created time.Time, ready bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              BuildPoolPodName(shard, qrPool, qrCell, idx),
			Namespace:         qrNS,
			Labels:            buildPoolLabelsWithCell(shard, qrPool, qrCell),
			CreationTimestamp: metav1.NewTime(created),
		},
	}
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
	}
	return pod
}

func qrPVC(shard *multigresv1alpha1.Shard, idx int) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BuildPoolDataPVCName(shard, qrPool, qrCell, idx),
			Namespace: qrNS,
			Labels:    buildPoolLabelsWithCell(shard, qrPool, qrCell),
		},
	}
}

// qrStoreWithQuarantined returns a memory topo store with the pooler backing the
// given pod indexes registered as LIFECYCLE_QUARANTINED (reason=qrReason), plus a
// healthy primary.
func qrStoreWithQuarantined(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	quarantinedIdxs ...int,
) topoclient.Store {
	return qrStoreWithQuarantinedReason(t, shard, qrReason, quarantinedIdxs...)
}

// qrStoreWithQuarantinedReason is qrStoreWithQuarantined with an explicit reason
// on the quarantined records (use "" to exercise the empty-reason fallback).
func qrStoreWithQuarantinedReason(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	reason string,
	quarantinedIdxs ...int,
) topoclient.Store {
	t.Helper()
	_, factory := memorytopo.NewServerAndFactory(context.Background(), qrCell)
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	t.Cleanup(func() { _ = store.Close() })

	q := map[int]bool{}
	for _, i := range quarantinedIdxs {
		q[i] = true
	}
	for idx := 0; idx < 2; idx++ {
		name := BuildPoolPodName(shard, qrPool, qrCell, idx)
		mp := &clustermetadata.Multipooler{
			Id:       &clustermetadata.ID{Cell: qrCell, Name: name},
			Hostname: name,
			RoutingState: &clustermetadata.RoutingState{
				Role: clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
			},
			ShardKey: &clustermetadata.ShardKey{
				Database: "testdb", TableGroup: "default", Shard: "shard0",
			},
		}
		if q[idx] {
			mp.LifecycleStatus = &clustermetadata.PoolerLifecycle{
				Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED,
				Reason: reason,
			}
		}
		if err := store.RegisterMultipooler(context.Background(), mp, false); err != nil {
			t.Fatalf("register pooler %s: %v", name, err)
		}
	}
	return store
}

func qrReconciler(t *testing.T, objs ...client.Object) *ShardReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		WithObjects(objs...).
		Build()
	return &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
}

func exists(t *testing.T, r *ShardReconciler, obj client.Object, key client.ObjectKey) bool {
	t.Helper()
	err := r.Get(context.Background(), key, obj)
	if err == nil {
		return true
	}
	if apierrors.IsNotFound(err) {
		return false
	}
	t.Fatalf("unexpected get error for %s: %v", key, err)
	return false
}

func TestReconcileQuarantineRemediation(t *testing.T) {
	old := time.Now().Add(-1 * time.Hour)

	t.Run("wipes a quarantined replica when pool healthy", func(t *testing.T) {
		shard := qrShard()
		markBackupHealthy(shard)
		badPod := qrPod(shard, 1, old, false) // quarantined replica, old, not ready
		goodPod := qrPod(shard, 0, old, true) // healthy sibling
		badPVC := qrPVC(shard, 1)
		r := qrReconciler(t, shard, badPod, goodPod, badPVC)
		store := qrStoreWithQuarantined(t, shard, 1)

		acted, err := r.reconcileQuarantineRemediation(context.Background(), store, shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !acted {
			t.Fatal("expected remediation to act on the quarantined pod")
		}
		if exists(t, r, &corev1.Pod{}, client.ObjectKeyFromObject(badPod)) {
			t.Error("expected quarantined pod to be deleted")
		}
		if exists(t, r, &corev1.PersistentVolumeClaim{}, client.ObjectKeyFromObject(badPVC)) {
			t.Error("expected quarantined pod's data PVC to be deleted (wiped)")
		}
		if !exists(t, r, &corev1.Pod{}, client.ObjectKeyFromObject(goodPod)) {
			t.Error("healthy sibling pod should be untouched")
		}

		// The remediation event should carry the topology quarantine reason.
		rec := r.Recorder.(*record.FakeRecorder)
		foundReason := false
		for len(rec.Events) > 0 {
			if ev := <-rec.Events; strings.Contains(ev, qrReason) {
				foundReason = true
			}
		}
		if !foundReason {
			t.Errorf("expected a remediation event containing the quarantine reason %q", qrReason)
		}
	})

	t.Run("wipes with an empty reason -> event falls back to 'unspecified'", func(t *testing.T) {
		shard := qrShard()
		markBackupHealthy(shard)
		badPod := qrPod(shard, 1, old, false)
		goodPod := qrPod(shard, 0, old, true)
		badPVC := qrPVC(shard, 1)
		r := qrReconciler(t, shard, badPod, goodPod, badPVC)
		store := qrStoreWithQuarantinedReason(t, shard, "", 1) // no reason recorded

		acted, err := r.reconcileQuarantineRemediation(context.Background(), store, shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !acted {
			t.Fatal("expected remediation to act even without a recorded reason")
		}
		if exists(t, r, &corev1.Pod{}, client.ObjectKeyFromObject(badPod)) {
			t.Error("expected quarantined pod to be deleted")
		}

		rec := r.Recorder.(*record.FakeRecorder)
		foundUnspecified := false
		for len(rec.Events) > 0 {
			if ev := <-rec.Events; strings.Contains(ev, "reason: unspecified") {
				foundUnspecified = true
			}
		}
		if !foundUnspecified {
			t.Error("expected the event to fall back to 'unspecified' on empty reason")
		}
	})

	t.Run("defers when the pod is too young (stale-record guard)", func(t *testing.T) {
		shard := qrShard()
		markBackupHealthy(shard)
		badPod := qrPod(shard, 1, time.Now(), false) // just created
		goodPod := qrPod(shard, 0, old, true)
		badPVC := qrPVC(shard, 1)
		r := qrReconciler(t, shard, badPod, goodPod, badPVC)
		store := qrStoreWithQuarantined(t, shard, 1)

		acted, err := r.reconcileQuarantineRemediation(context.Background(), store, shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if acted {
			t.Error("expected no action for a too-young pod")
		}
		if !exists(t, r, &corev1.Pod{}, client.ObjectKeyFromObject(badPod)) {
			t.Error("young quarantined pod must not be deleted yet")
		}
		if !exists(t, r, &corev1.PersistentVolumeClaim{}, client.ObjectKeyFromObject(badPVC)) {
			t.Error("young quarantined pod's PVC must not be deleted yet")
		}
	})

	t.Run("defers when another pod in the pool is unhealthy", func(t *testing.T) {
		shard := qrShard()
		markBackupHealthy(shard)
		badPod := qrPod(shard, 1, old, false)  // quarantined
		sickPod := qrPod(shard, 0, old, false) // non-quarantined but not ready
		badPVC := qrPVC(shard, 1)
		r := qrReconciler(t, shard, badPod, sickPod, badPVC)
		store := qrStoreWithQuarantined(t, shard, 1) // only idx 1 quarantined

		acted, err := r.reconcileQuarantineRemediation(context.Background(), store, shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if acted {
			t.Error("expected no action while another pool pod is unhealthy")
		}
		if !exists(t, r, &corev1.Pod{}, client.ObjectKeyFromObject(badPod)) {
			t.Error("quarantined pod must not be wiped during a broader outage")
		}
	})

	t.Run("defers when no healthy backup exists (never wipe the last copy)", func(t *testing.T) {
		shard := qrShard() // no backup-health condition => not healthy
		badPod := qrPod(shard, 1, old, false)
		goodPod := qrPod(shard, 0, old, true)
		badPVC := qrPVC(shard, 1)
		r := qrReconciler(t, shard, badPod, goodPod, badPVC)
		store := qrStoreWithQuarantined(t, shard, 1)

		acted, err := r.reconcileQuarantineRemediation(context.Background(), store, shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if acted {
			t.Error("expected no action when there is no healthy backup to restore from")
		}
		if !exists(t, r, &corev1.Pod{}, client.ObjectKeyFromObject(badPod)) {
			t.Error("must not delete the pod without a healthy backup")
		}
		if !exists(t, r, &corev1.PersistentVolumeClaim{}, client.ObjectKeyFromObject(badPVC)) {
			t.Error("must not wipe the data PVC without a healthy backup")
		}
	})

	t.Run("no quarantined poolers -> no action", func(t *testing.T) {
		shard := qrShard()
		p0 := qrPod(shard, 0, old, true)
		p1 := qrPod(shard, 1, old, true)
		r := qrReconciler(t, shard, p0, p1)
		store := qrStoreWithQuarantined(t, shard) // none quarantined

		acted, err := r.reconcileQuarantineRemediation(context.Background(), store, shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if acted {
			t.Error("expected no action when nothing is quarantined")
		}
	})
}
