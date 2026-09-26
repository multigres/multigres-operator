//go:build integration

package toposerver

import (
	"path/filepath"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/testkit/assert"
)

func TestMaintenanceReservationSurvivesStatusApply(t *testing.T) {
	ck := assert.NewAborting(t)
	scheme := certScheme()
	_ = appsv1.AddToScheme(scheme)
	cfg := testutil.SetUpEnvtest(
		t,
		testutil.WithCRDPaths(filepath.Join("../../../..", "config", "crd", "bases")),
	)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	ck.NoError(err)
	ts := certTestTopoServer(nil)
	ts.Namespace = "default"
	ts.UID = ""
	ck.NoError(c.Create(t.Context(), ts))
	sts, err := BuildStatefulSet(ts, scheme)
	ck.NoError(err)
	ck.NoError(c.Create(t.Context(), sts))
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
	ck.NoError(r.saveMaintenance(t.Context(), ts, state))
	if err := r.saveMaintenance(t.Context(), stale, state); !apierrors.IsConflict(err) {
		t.Fatalf("stale reservation should conflict, got %v", err)
	}
	// The ordinary status writer intentionally omits maintenance fields; SSA
	// must preserve the independently owned reservation, including after restart.
	ck.NoError(r.updateStatus(t.Context(), stale))
	fresh := &multigresv1alpha1.TopoServer{}
	ck.NoError(c.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh))
	ck.False(
		fresh.Status.EtcdMaintenance == nil || !fresh.Status.EtcdMaintenance.InProgress,
		"status apply removed active maintenance reservation",
	)
}
