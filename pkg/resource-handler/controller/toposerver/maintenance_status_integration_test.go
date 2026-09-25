//go:build integration

package toposerver

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestMaintenanceReservationSurvivesStatusApply(t *testing.T) {
	scheme := certScheme()
	_ = appsv1.AddToScheme(scheme)
	cfg := testutil.SetUpEnvtest(
		t,
		testutil.WithCRDPaths(filepath.Join("../../../..", "config", "crd", "bases")),
	)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ts := certTestTopoServer(nil)
	ts.Namespace = "default"
	ts.UID = ""
	if err := c.Create(t.Context(), ts); err != nil {
		t.Fatal(err)
	}
	sts, err := BuildStatefulSet(ts, scheme)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Create(t.Context(), sts); err != nil {
		t.Fatal(err)
	}
	r := &TopoServerReconciler{
		Client:    c,
		APIReader: c,
		Scheme:    scheme,
		Recorder:  record.NewFakeRecorder(100),
	}
	stale := ts.DeepCopy()
	state := &multigresv1alpha1.EtcdMaintenanceStatus{
		LastAttemptTime: metav1.Now(),
		Endpoint:        maintenanceEndpoints(ts)[0],
		InProgress:      true,
	}
	if err := r.saveMaintenance(t.Context(), ts, state); err != nil {
		t.Fatal(err)
	}
	if err := r.saveMaintenance(t.Context(), stale, state); !apierrors.IsConflict(err) {
		t.Fatalf("stale reservation should conflict, got %v", err)
	}

	r.newMaintenanceClient = func(context.Context, *multigresv1alpha1.TopoServer) (etcdMaintenanceClient, error) {
		return nil, errors.New("credential unavailable")
	}
	if _, err := r.reconcileHealth(
		t.Context(),
		ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ts)},
	); err != nil {
		t.Fatal(err)
	}
	// The ordinary status writer intentionally omits maintenance fields; SSA
	// must preserve the independently owned reservation, including after restart.
	if err := r.updateStatus(t.Context(), stale); err != nil {
		t.Fatal(err)
	}
	fresh := &multigresv1alpha1.TopoServer{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh); err != nil {
		t.Fatal(err)
	}
	if fresh.Status.HealthCheckedAt == nil ||
		meta.FindStatusCondition(fresh.Status.Conditions, "QuorumAvailable") == nil {
		t.Fatal("resource status apply removed health observation")
	}
	if fresh.Status.EtcdMaintenance == nil || !fresh.Status.EtcdMaintenance.InProgress {
		t.Fatal("status apply removed active maintenance reservation")
	}
}
