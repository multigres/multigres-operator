//go:build integration
// +build integration

package toposerver_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/multigres/testkit/managedfields"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	toposervercontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/toposerver"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// operatorManagers is the managers on obj the operator owns: the creating test
// client and the apiserver appear under other names and are not the subject.
func operatorManagers(obj client.Object) []string {
	var out []string
	for _, m := range managedfields.Managers(obj) {
		if strings.HasPrefix(m, "multigres-") {
			out = append(out, m)
		}
	}
	return out
}

// requireOnlyManagers asserts got names main and nothing but main and the
// facets, and no legacy name.
func requireOnlyManagers(t *testing.T, got []string, main string, facets ...string) {
	t.Helper()
	require.Contains(t, got, main)
	allowed := append([]string{main}, facets...)
	for _, m := range got {
		require.Contains(t, allowed, m, "unexpected operator field manager %q", m)
		require.False(t, ownership.IsLegacy(m), "legacy field manager %q present", m)
	}
}

// legacyManagers is the legacy-named managers on obj, sorted.
func legacyManagers(obj client.Object) []string {
	var out []string
	for _, m := range managedfields.Managers(obj) {
		if ownership.IsLegacy(m) {
			out = append(out, m)
		}
	}
	return out
}

func TestTopoServer_WritesUnderOwnManager(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths("../../../../config/crd/bases"))
	raw := testutil.SetUpClient(t, cfg, scheme)
	mgr := testutil.SetUpManager(t, cfg, scheme)
	if err := (&toposervercontroller.TopoServerReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("toposerver-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}); err != nil {
		t.Fatal(err)
	}

	ts := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "own-toposerver", Namespace: "default",
			Labels: map[string]string{"multigres.com/cluster": "own-cluster"},
		},
		Spec: multigresv1alpha1.TopoServerSpec{Etcd: &multigresv1alpha1.EtcdSpec{}},
	}
	ctx := t.Context()
	k8s := raw
	// Real server-side applies under the legacy name, on the main resource and
	// on status, as the pre-rename operator left the object. A hand-built
	// managedFields would encode what the test expects of the apiserver. The
	// controller is not running yet, so its reconcile cannot race the seed.
	require.NoError(t, managedfields.Seed(ctx, k8s, ts, ownership.LegacyOperator))
	ts.Status.ObservedGeneration = 1
	require.NoError(
		t,
		managedfields.Seed(ctx, k8s, ts, ownership.LegacyOperator, managedfields.OnStatus()),
	)
	require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(ts), ts))
	entries, err := managedfields.Entries(ts)
	require.NoError(t, err)
	var seededOn []string
	for _, e := range entries {
		if ownership.IsLegacy(e.Manager) {
			seededOn = append(seededOn, e.Subresource)
		}
	}
	require.Contains(t, seededOn, "", "seed left no legacy entry on the main resource")
	require.Contains(t, seededOn, "status", "seed left no legacy entry on status")
	seededSpec := ts.Spec.DeepCopy()
	testutil.StartManager(t, mgr)

	require.Eventually(t, func() bool {
		got := &multigresv1alpha1.TopoServer{}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(ts), got); err != nil {
			return false
		}
		return slices.Contains(operatorManagers(got), ownership.ManagerTopoServer)
	}, 10*time.Second, 100*time.Millisecond, "TopoServer never carried its own manager")

	got := &multigresv1alpha1.TopoServer{}
	require.Eventually(t, func() bool {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(ts), got); err != nil {
			return false
		}
		return len(legacyManagers(got)) == 0
	}, 15*time.Second, 100*time.Millisecond, "legacy field manager entries never dropped")
	require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(ts), got))
	require.Equal(t, *seededSpec, got.Spec, "spec changed across the legacy drop")
	requireOnlyManagers(
		t,
		operatorManagers(got),
		ownership.ManagerTopoServer,
		ownership.ManagerTopoServerGuard,
		ownership.ManagerEtcdMaintenance,
	)
}
