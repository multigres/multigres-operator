//go:build integration
// +build integration

package cell_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	cellcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// TestCell_PendingDeletionAnnotationAloneEnqueues asserts that setting only the
// PendingDeletion annotation on a Cell, with nothing else about it changing, is
// enough for the controller's own watch to enqueue it and for
// handlePendingDeletion to run. It deliberately never calls Reconcile
// directly: the bug this pins lives in the predicate the manager's watch
// evaluates before a request ever reaches Reconcile, so a direct call would
// pass regardless of the predicate and prove nothing.
//
// Only CellReconciler is registered against this envtest manager. Its own
// SetupWithManager still watches the Multigateway Deployment and Service it
// owns (Owns() events are not filtered by the For predicate), so a
// Deployment or Service write could re-enqueue the Cell and rescue the bug
// the same way an owned Shard rescues a TableGroup in production, and a Cell
// that is not Healthy reschedules itself, which would rescue it too. The Cell
// built below has no managed local TopoServer (Spec.TopoServer left nil), so
// its pending-deletion path resolves without waiting on any child; the test
// brings it to Healthy and lets the reconciles that triggered settle before
// annotating, so the only path left that can flip ReadyForDeletion is the
// manager's watch evaluating the For predicate against the annotation-only
// update itself.
//
// test/suite's TestReadyForDeletionProtocol_OrphanCellDrains pins the same
// bug across all five controllers, with quiescence established by the
// harness instead of a time margin; this one isolates the Cell controller.
func TestCell_PendingDeletionAnnotationAloneEnqueues(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)

	if err := (&cellcontroller.CellReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("cell-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}); err != nil {
		t.Fatal(err)
	}

	k8sClient := mgr.GetClient()
	ctx := t.Context()

	const (
		namespace = "default"
		cellName  = "annotation-enqueue-cell"
	)

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cellName,
			Namespace: namespace,
			Labels:    map[string]string{"multigres.com/cluster": "annotation-enqueue-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Pgctld:       "info",
				Multipooler:  "info",
				Multiorch:    "info",
				Multiadmin:   "info",
				Multigateway: "info",
			},
			ZoneID: "usw1-az1",
			Images: multigresv1alpha1.CellImages{
				Multigateway: "ghcr.io/multigres/multigres:main",
			},
			Multigateway: multigresv1alpha1.MultigatewaySpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
		},
	}
	if err := k8sClient.Create(ctx, cell); err != nil {
		t.Fatal(err)
	}

	deploymentKey := client.ObjectKey{
		Name:      cellcontroller.BuildMultigatewayDeploymentName(cell),
		Namespace: namespace,
	}
	// envtest runs no Deployment controller, so the gateway never reports
	// ready on its own and the Cell would sit in Progressing, rescheduling
	// itself every statusRecheckDelay. That requeue alone would eventually
	// run handlePendingDeletion with the predicate broken, so mark the
	// Deployment ready, the way its controller would, and wait for the Cell
	// to reach Healthy: a Healthy Cell asks for no requeue.
	pollUntil(t, 10*time.Second, "Cell to reach Healthy", func() error {
		var dep appsv1.Deployment
		if err := k8sClient.Get(ctx, deploymentKey, &dep); err != nil {
			return err
		}
		replicas := ptr.Deref(dep.Spec.Replicas, 1)
		if dep.Status.ReadyReplicas != replicas ||
			dep.Status.ObservedGeneration != dep.Generation {
			dep.Status.Replicas = replicas
			dep.Status.ReadyReplicas = replicas
			dep.Status.AvailableReplicas = replicas
			dep.Status.UpdatedReplicas = replicas
			dep.Status.ObservedGeneration = dep.Generation
			if err := k8sClient.Status().Update(ctx, &dep); err != nil {
				return err
			}
		}
		var fresh multigresv1alpha1.Cell
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cell), &fresh); err != nil {
			return err
		}
		if fresh.Status.Phase != multigresv1alpha1.PhaseHealthy {
			return fmt.Errorf("phase is %q", fresh.Status.Phase)
		}
		return nil
	})

	// The Deployment status write above enqueues the Cell through
	// Owns(Deployment), and that pass can still be running, or about to
	// rerun, when the Healthy phase becomes visible. If it read the Cell
	// after the annotation patch below, it would drain the Cell with the
	// predicate broken and the test would pass for the wrong reason. There
	// is no reconcile boundary to wait on in a bare envtest manager, so this
	// waits a margin instead: too short a margin can only let a broken
	// predicate pass, never fail a correct one.
	time.Sleep(3 * time.Second)

	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cell), cell); err != nil {
		t.Fatal(err)
	}
	if meta.IsStatusConditionTrue(
		cell.Status.Conditions,
		multigresv1alpha1.ConditionReadyForDeletion,
	) {
		t.Fatal("Cell already reports ReadyForDeletion before it was ever annotated")
	}
	generationBefore := cell.Generation

	// Stamp only the PendingDeletion annotation, the same write
	// multigrescluster's orphan-pruning loop makes on a Cell that fell out of
	// the cluster spec. A merge patch touches nothing else: no spec field, no
	// other annotation.
	patch := client.MergeFrom(cell.DeepCopy())
	if cell.Annotations == nil {
		cell.Annotations = make(map[string]string)
	}
	cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = metav1.Now().
		UTC().
		Format(time.RFC3339)
	if err := k8sClient.Patch(ctx, cell, patch); err != nil {
		t.Fatalf("failed to set PendingDeletion annotation: %v", err)
	}
	if cell.Generation != generationBefore {
		t.Fatalf(
			"annotation patch changed Generation: got %d, want %d (test setup invalid)",
			cell.Generation, generationBefore,
		)
	}

	// The only way ReadyForDeletion can now flip True is this Cell's own
	// reconcile running handlePendingDeletion, and the only way that
	// reconcile gets enqueued is the manager's watch evaluating the For
	// predicate against the annotation-only update just made. Nothing else
	// touches the Cell from here.
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		var fresh multigresv1alpha1.Cell
		if err := k8sClient.Get(waitCtx, client.ObjectKeyFromObject(cell), &fresh); err == nil &&
			meta.IsStatusConditionTrue(
				fresh.Status.Conditions,
				multigresv1alpha1.ConditionReadyForDeletion,
			) {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf(
				"Cell %s never reported ReadyForDeletion after its own annotation-only "+
					"update; the For predicate dropped the event and nothing else "+
					"re-enqueued it: %v",
				cellName, waitCtx.Err(),
			)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// pollUntil retries check every 200ms until it returns nil, failing the test
// with the last error once timeout passes.
func pollUntil(t *testing.T, timeout time.Duration, what string, check func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := check()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: %v", what, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
