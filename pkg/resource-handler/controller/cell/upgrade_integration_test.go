//go:build integration
// +build integration

package cell_test

import (
	"slices"
	"testing"
	"time"

	"github.com/multigres/testkit/managedfields"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	cellcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

const upgradeAnnotation = "test.multigres.com/dropped"

// TestCell_UpgradeKeepsChildWriterAndPrunes drives the Cell controller over
// objects in the state an upgrade leaves them in: the Cell's spec and the
// gateway Deployment applied under the child writer's name, the Cell's status
// under the same name on the status subresource. It then changes several spec
// fields in one update: a pod annotation removed, and the CPU request raised
// past the old limit with the limit removed.
//
// Under the same child-writer name the controller's apply replaces its own
// entry, so the annotation is pruned and the limit goes with it. Under any
// other name the old entry would stay a co-owner: the annotation would
// survive, and the merged object (request 200m, limit 100m) would fail
// validation on every apply.
func TestCell_UpgradeKeepsChildWriterAndPrunes(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths("../../../../config/crd/bases"))
	k8s := testutil.SetUpClient(t, cfg, scheme)
	mgr := testutil.SetUpManager(t, cfg, scheme)
	require.NoError(t, (&cellcontroller.CellReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("cell-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}))

	cpu := func(v string) corev1.ResourceList {
		return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(v)}
	}
	cell := &multigresv1alpha1.Cell{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "Cell",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "upgrade-cell", Namespace: "default",
			Labels: map[string]string{"multigres.com/cluster": "upgrade-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name:   "zone1",
			ZoneID: "usw1-az1",
			Images: multigresv1alpha1.CellImages{Multigateway: "ghcr.io/multigres/multigres:main"},
			Multigateway: multigresv1alpha1.MultigatewaySpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{
					Replicas:       ptr.To(int32(1)),
					PodAnnotations: map[string]string{upgradeAnnotation: "x"},
					Resources: corev1.ResourceRequirements{
						Requests: cpu("100m"),
						Limits:   cpu("100m"),
					},
				},
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address: "global-topo:2379", RootPath: "/multigres/global", Implementation: "etcd",
			},
		},
	}
	ctx := t.Context()
	// The parent applies a Cell's spec under the child writer's name.
	require.NoError(t, managedfields.Seed(ctx, k8s, cell, ownership.ManagerChildWriter))
	seeded := cell.DeepCopy()
	seeded.Status = multigresv1alpha1.CellStatus{ObservedGeneration: 1}
	require.NoError(t, managedfields.Seed(ctx, k8s, seeded, ownership.ManagerChildWriter,
		managedfields.OnStatus()))
	testutil.StartManager(t, mgr)

	key := client.ObjectKey{
		Namespace: cell.Namespace,
		Name:      cellcontroller.BuildMultigatewayDeploymentName(cell),
	}
	deploy := &appsv1.Deployment{}
	require.Eventually(t, func() bool {
		return k8s.Get(ctx, key, deploy) == nil &&
			deploy.Spec.Template.Annotations[upgradeAnnotation] == "x"
	}, 15*time.Second, 100*time.Millisecond, "gateway Deployment never created")
	testutil.RewriteAsChildWriter(t, k8s, deploy)

	// One update, several fields: the annotation removed, the request raised
	// and the limit removed.
	require.Eventually(t, func() bool {
		cur := &multigresv1alpha1.Cell{}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(cell), cur); err != nil {
			return false
		}
		cur.Spec.Multigateway.PodAnnotations = nil
		cur.Spec.Multigateway.Resources = corev1.ResourceRequirements{Requests: cpu("200m")}
		return k8s.Update(ctx, cur, client.FieldOwner(ownership.ManagerChildWriter)) == nil
	}, 10*time.Second, 100*time.Millisecond, "update the Cell spec")

	require.Eventually(t, func() bool {
		if err := k8s.Get(ctx, key, deploy); err != nil {
			return false
		}
		c := deploy.Spec.Template.Spec.Containers[0]
		_, annotated := deploy.Spec.Template.Annotations[upgradeAnnotation]
		_, limited := c.Resources.Limits[corev1.ResourceCPU]
		return !annotated && !limited &&
			c.Resources.Requests.Cpu().Cmp(resource.MustParse("200m")) == 0
	}, 20*time.Second, 100*time.Millisecond,
		"gateway Deployment never dropped the annotation and the CPU limit: %+v",
		deploy.Spec.Template)
	require.Equal(t, []string{ownership.ManagerChildWriter}, testutil.OperatorManagers(deploy))

	// Status moved to the Cell's own manager; the spec entry the parent wrote
	// under the child writer's name is live and stays.
	require.Eventually(t, func() bool {
		got := &multigresv1alpha1.Cell{}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(cell), got); err != nil {
			return false
		}
		has := func(manager, sub string) bool {
			return slices.ContainsFunc(got.ManagedFields, func(e metav1.ManagedFieldsEntry) bool {
				return e.Manager == manager && e.Subresource == sub
			})
		}
		return has(ownership.ManagerCell, "status") &&
			!has(ownership.ManagerChildWriter, "status") &&
			has(ownership.ManagerChildWriter, "")
	}, 15*time.Second, 100*time.Millisecond, "Cell status never moved to its own manager")
}
