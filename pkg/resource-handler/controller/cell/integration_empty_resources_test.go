//go:build integration

package cell_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	cellcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// TestEmptyResourcesBlockReachesLocalTopoServer writes each empty resources
// shape (such as `{limits: {}}`) into a Cell's local etcd block that had
// entries, and requires the Cell controller's apply of the managed local
// TopoServer to drop the block. An apply that sent `resources: {}` over a
// block that had entries is rejected by the API server, which would leave the
// TopoServer with the old values. Each shape is followed by a return to
// entries.
func TestEmptyResourcesBlockReachesLocalTopoServer(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	require.NoError(t, (&cellcontroller.CellReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("cell-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: new(true)}))
	c := mgr.GetClient()
	ctx := t.Context()

	populated := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("123m")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("321Mi")},
	}
	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "empty-resources-cell",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "empty-resources"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name:   "zone1",
			ZoneID: "usw1-az1",
			Images: multigresv1alpha1.CellImages{
				Multigateway: "ghcr.io/multigres/multigres:main",
			},
			Multigateway: multigresv1alpha1.MultigatewaySpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: new(int32(1))},
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd:local", Resources: populated},
			},
		},
	}
	require.NoError(t, c.Create(ctx, cell))
	topoKey := client.ObjectKey{
		Namespace: cell.Namespace,
		Name:      cellcontroller.BuildLocalTopoServerName(cell),
	}

	setResources := func(shape map[string]any) {
		t.Helper()
		require.NoError(t, testutil.UpdateUnstructured(ctx, c,
			multigresv1alpha1.GroupVersion.WithKind("Cell"),
			client.ObjectKeyFromObject(cell),
			func(content map[string]any) error {
				value := (&unstructured.Unstructured{Object: shape}).DeepCopy().Object
				return testutil.SetUnstructuredPath(content,
					[]any{"spec", "topoServer", "etcd", "resources"}, value)
			}))
	}
	requireTopoResources := func(want bool) {
		t.Helper()
		var last corev1.ResourceRequirements
		var lastErr error
		if err := wait.PollUntilContextTimeout(t.Context(), 200*time.Millisecond, 30*time.Second, true,
			func(ctx context.Context) (bool, error) {
				ts := &multigresv1alpha1.TopoServer{}
				err := c.Get(ctx, topoKey, ts)
				lastErr = err
				if err != nil || ts.Spec.Etcd == nil {
					return false, nil
				}
				last = ts.Spec.Etcd.Resources
				if want {
					return last.Requests.Cpu().String() == "123m" &&
						last.Limits.Memory().String() == "321Mi", nil
				}
				return len(last.Requests) == 0 && len(last.Limits) == 0 && len(last.Claims) == 0, nil
			}); err != nil {
			testutil.LogWarnings(t, c, "default", "")
			t.Fatalf("local TopoServer never converged (want populated=%v): %v, last read error %v",
				want, last, lastErr)
		}
	}

	populatedJSON, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&populated)
	require.NoError(t, err)

	requireTopoResources(true)
	for _, s := range []struct {
		name  string
		shape map[string]any
	}{
		{"empty requests", map[string]any{"requests": map[string]any{}}},
		{"empty limits", map[string]any{"limits": map[string]any{}}},
		{"empty claims", map[string]any{"claims": []any{}}},
		{"all empty", map[string]any{
			"requests": map[string]any{},
			"limits":   map[string]any{},
			"claims":   []any{},
		}},
		{"empty block", map[string]any{}},
	} {
		setResources(s.shape)
		t.Logf("shape %s: waiting for the TopoServer to drop its resources", s.name)
		requireTopoResources(false)

		setResources(populatedJSON)
		t.Logf("shape %s: waiting for the TopoServer to carry resources again", s.name)
		requireTopoResources(true)
	}
}
