//go:build integration

package shard

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// TestReloadTargetStableAcrossReapply verifies the fix for the SSA-clobbering
// bug end to end against a real API server: applyPostgresConfigMap stamps the
// reload target, and re-applying the ConfigMap (as happens every reconcile) must
// (a) keep the reload annotations and (b) NOT advance updated-at while the target
// is unchanged — otherwise the reload sync-lag gate would reset forever.
func TestReloadTargetStableAcrossReapply(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	mgr := testutil.SetUpEnvtestManager(t, scheme)

	r := &ShardReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	}
	ctx := context.Background()

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "gate-shard", Namespace: "default", UID: "gate-shard-uid"},
	}
	cmName := PostgresConfigMapName("gate-shard")

	read := func() *corev1.ConfigMap {
		cm := &corev1.ConfigMap{}
		if err := r.APIReader.Get(ctx, types.NamespacedName{Name: cmName, Namespace: "default"}, cm); err != nil {
			t.Fatalf("get CM: %v", err)
		}
		return cm
	}

	// First delivery stamps the target.
	if err := r.applyPostgresConfigMap(ctx, shard, "work_mem = '8MB'\n", "R2"); err != nil {
		t.Fatalf("apply 1: %v", err)
	}
	first := read()
	if first.Annotations[metadata.AnnotationPostgresReloadHash] != "R2" {
		t.Fatalf("reload-hash not stamped: %v", first.Annotations)
	}
	stamp1 := first.Annotations[metadata.AnnotationPostgresReloadHashUpdatedAt]
	if stamp1 == "" {
		t.Fatal("updated-at not stamped")
	}

	// Re-apply several times with the SAME reload target (simulating steady-state
	// reconciles). The annotation must survive and updated-at must not move.
	for i := 0; i < 3; i++ {
		if err := r.applyPostgresConfigMap(ctx, shard, "work_mem = '8MB'\n", "R2"); err != nil {
			t.Fatalf("apply re-%d: %v", i, err)
		}
		cm := read()
		if cm.Annotations[metadata.AnnotationPostgresReloadHash] != "R2" {
			t.Fatalf("re-apply %d stripped reload-hash: %v", i, cm.Annotations)
		}
		if got := cm.Annotations[metadata.AnnotationPostgresReloadHashUpdatedAt]; got != stamp1 {
			t.Fatalf("re-apply %d advanced updated-at %q -> %q (gate would reset forever)", i, stamp1, got)
		}
	}

	// A change to the reload target DOES advance updated-at (resetting the wait).
	if err := r.applyPostgresConfigMap(ctx, shard, "work_mem = '16MB'\n", "R3"); err != nil {
		t.Fatalf("apply new target: %v", err)
	}
	changed := read()
	if changed.Annotations[metadata.AnnotationPostgresReloadHash] != "R3" {
		t.Fatalf("new target not stamped: %v", changed.Annotations)
	}
	if changed.Annotations[metadata.AnnotationPostgresReloadHashUpdatedAt] == stamp1 {
		t.Errorf("updated-at did not advance on a target change (still %q)", stamp1)
	}
}
