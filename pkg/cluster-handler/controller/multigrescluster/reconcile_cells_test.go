package multigrescluster

import (
	"context"
	"errors"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/multigres/testkit/assert"
)

func TestReconcileCells_ErrorPaths(t *testing.T) {
	scheme := setupScheme()

	t.Run("Error: Get Global Topo Ref Failed", func(t *testing.T) {
		// Use explicit invalid core template to force GetGlobalTopoRef to fail
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					TemplateRef: "non-existent-core",
				},
				Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		_, err := r.reconcileCells(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		assert.NewCollecting(t).Error(err, "Expected error due to missing global topo, got nil")
	})

	t.Run("Error: Resolve Cell Failed", func(t *testing.T) {
		// Use explicit invalid cell template
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd"},
				},
				Cells: []multigresv1alpha1.CellConfig{{
					Name:         "zone-a",
					ZoneID:       "use1-az1",
					CellTemplate: "non-existent-cell",
				}},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		_, err := r.reconcileCells(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		assert.NewCollecting(t).Error(err, "Expected error due to missing cell template, got nil")
	})

	t.Run("Error: List Existing Cells Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cli client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				return errors.New("list error")
			},
		}).WithObjects(cluster).Build()

		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		_, err := r.reconcileCells(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		assert.NewCollecting(t).
			False(err == nil || err.Error() != "failed to list existing cells: list error", "Expected 'list error', got %v", err)
	})

	t.Run("Error: Patch Cell Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd"},
				},
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "zone-a", ZoneID: "use1-az1"},
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return errors.New("patch error")
			},
		}).WithObjects(cluster).Build()

		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		_, err := r.reconcileCells(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		assert.NewCollecting(t).
			False(err == nil || err.Error() != "failed to apply cell 'zone-a': patch error", "Expected 'patch error', got %v", err)
	})

	t.Run("Error: Set PendingDeletion on Orphaned Cell Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			// No cells in spec -> existing cell should get PendingDeletion
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd"},
				},
			},
		}
		existingCell := &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-zone-orphan",
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test"},
			},
			Spec: multigresv1alpha1.CellSpec{Name: "zone-orphan"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return errors.New("patch error")
			},
		}).WithObjects(cluster, existingCell).Build()

		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		_, err := r.reconcileCells(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		assert.NewCollecting(t).False(err == nil ||
			err.Error() != "failed to set PendingDeletion on cell 'test-zone-orphan': patch error", "Expected PendingDeletion patch error, got %v", err)
	})

	t.Run("Error: Build Cell Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd"},
				},
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "zone-a", ZoneID: "use1-az1"},
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		// Use empty scheme for Reconciler to cause SetControllerReference to fail
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   runtime.NewScheme(),
			Recorder: record.NewFakeRecorder(10),
		}

		_, err := r.reconcileCells(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		assert.NewCollecting(t).
			Error(err, "Expected error due to build failure (scheme mismatch), got nil")
	})
}

func TestReconcileCells_HappyPath(t *testing.T) {
	scheme := setupScheme()

	t.Run("Happy Path: Create Cells and Mark Orphan PendingDeletion", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd"},
				},
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "zone-new", ZoneID: "use1-az1"},
				},
			},
		}
		// Existing cell that should be marked for pending deletion
		orphan := &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-zone-old",
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test"},
			},
			Spec: multigresv1alpha1.CellSpec{Name: "zone-old"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, orphan).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		pending, err := r.reconcileCells(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		ck.Require().NoError(err, "Expected happy path success, got")
		ck.True(pending, "Expected pending=true for orphan cell pending deletion")

		// Verify "zone-new" created and orphan has PendingDeletion annotation
		cells := &multigresv1alpha1.CellList{}
		ck.Require().NoError(c.List(context.Background(), cells))
		foundNew := false
		for _, cell := range cells.Items {
			if cell.Spec.Name == "zone-new" {
				foundNew = true
			}
			if cell.Spec.Name == "zone-old" {
				ck.NotEq(
					"",
					cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion],
					"Expected orphan cell 'zone-old' to have PendingDeletion annotation",
				)
			}
		}
		ck.True(foundNew, "Expected new cell 'zone-new' to be created")
	})
}
