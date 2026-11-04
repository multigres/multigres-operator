package multigrescluster

import (
	"context"
	"errors"
	"strings"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
		if err == nil {
			t.Error("Expected error due to missing global topo, got nil")
		}
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
		if err == nil {
			t.Error("Expected error due to missing cell template, got nil")
		}
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
		if err == nil || err.Error() != "failed to list existing cells: list error" {
			t.Errorf("Expected 'list error', got %v", err)
		}
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
		if err == nil || err.Error() != "failed to apply cell 'zone-a': patch error" {
			t.Errorf("Expected 'patch error', got %v", err)
		}
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
		if err == nil ||
			err.Error() != "failed to set PendingDeletion on cell 'test-zone-orphan': patch error" {
			t.Errorf("Expected PendingDeletion patch error, got %v", err)
		}
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
		if err == nil {
			t.Error("Expected error due to build failure (scheme mismatch), got nil")
		}
	})
}

func TestReconcileCells_HappyPath(t *testing.T) {
	scheme := setupScheme()

	t.Run("Happy Path: Create Cells and Mark Orphan PendingDeletion", func(t *testing.T) {
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
		if err != nil {
			t.Fatalf("Expected happy path success, got %v", err)
		}
		if !pending {
			t.Error("Expected pending=true for orphan cell pending deletion")
		}

		// Verify "zone-new" created and orphan has PendingDeletion annotation
		cells := &multigresv1alpha1.CellList{}
		if err := c.List(context.Background(), cells); err != nil {
			t.Fatal(err)
		}
		foundNew := false
		for _, cell := range cells.Items {
			if cell.Spec.Name == "zone-new" {
				foundNew = true
			}
			if cell.Spec.Name == "zone-old" {
				if cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
					t.Error("Expected orphan cell 'zone-old' to have PendingDeletion annotation")
				}
			}
		}
		if !foundNew {
			t.Error("Expected new cell 'zone-new' to be created")
		}
	})
}

func TestReconcileCells_CancelsPendingDeletionOfRestoredCell(t *testing.T) {
	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd"},
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
		},
	}
	stampedCell := func(mutate func(*multigresv1alpha1.Cell)) *multigresv1alpha1.Cell {
		cell := &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "stamped-zone-a",
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test"},
				Annotations: map[string]string{
					multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
					"example.com/unrelated":                     "kept",
				},
			},
			Spec: multigresv1alpha1.CellSpec{Name: "zone-a"},
		}
		if mutate != nil {
			mutate(cell)
		}
		return cell
	}
	reconcile := func(t *testing.T, c client.Client) (bool, error) {
		t.Helper()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		return r.reconcileCells(context.Background(), cluster, resolver.NewResolver(c, "default"))
	}

	t.Run("clears the annotation of a Cell back in spec", func(t *testing.T) {
		cell := stampedCell(nil)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, cell).Build()
		pending, err := reconcile(t, c)
		if err != nil {
			t.Fatal(err)
		}
		if pending {
			t.Error("a Cell back in spec is not pending deletion")
		}
		got := &multigresv1alpha1.Cell{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(cell), got); err != nil {
			t.Fatal(err)
		}
		if v, ok := got.Annotations[multigresv1alpha1.AnnotationPendingDeletion]; ok {
			t.Errorf("PendingDeletion still set: %q", v)
		}
		if got.Annotations["example.com/unrelated"] != "kept" {
			t.Errorf("unrelated annotation lost: %v", got.Annotations)
		}
	})

	t.Run("leaves a terminating Cell alone", func(t *testing.T) {
		cell := stampedCell(func(c *multigresv1alpha1.Cell) {
			c.Finalizers = []string{"example.com/hold"}
			c.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
		})
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, cell).Build()
		if _, err := reconcile(t, c); err != nil {
			t.Fatal(err)
		}
		got := &multigresv1alpha1.Cell{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(cell), got); err != nil {
			t.Fatal(err)
		}
		if got.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
			t.Error("PendingDeletion cleared on a terminating Cell")
		}
	})

	t.Run("re-judges a Cell that changed since it was read", func(t *testing.T) {
		cell := stampedCell(nil)
		var patchBody string
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, cell).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(
					ctx context.Context,
					cl client.WithWatch,
					obj client.Object,
					patch client.Patch,
					opts ...client.PatchOption,
				) error {
					if patch.Type() != types.MergePatchType {
						return cl.Patch(ctx, obj, patch, opts...)
					}
					data, err := patch.Data(obj)
					if err != nil {
						return err
					}
					patchBody = string(data)
					return apierrors.NewConflict(
						schema.GroupResource{Group: "multigres.com", Resource: "cells"},
						obj.GetName(), errors.New("resourceVersion changed"))
				},
			}).Build()
		recorder := record.NewFakeRecorder(10)
		r := &MultigresClusterReconciler{Client: c, Scheme: scheme, Recorder: recorder}
		pending, err := r.reconcileCells(
			context.Background(), cluster, resolver.NewResolver(c, "default"))
		if err != nil {
			t.Fatalf("a conflicting cancel is a retry, not an error: %v", err)
		}
		if pending {
			t.Error("a Cell back in spec is not pending deletion")
		}
		if !strings.Contains(patchBody, `"resourceVersion"`) {
			t.Errorf("cancel patch is not conditioned on the version read: %s", patchBody)
		}
		for len(recorder.Events) > 0 {
			if ev := <-recorder.Events; strings.Contains(ev, "PendingDeletionCancelled") {
				t.Errorf("cancel event emitted for a cancel that did not land: %s", ev)
			}
		}
	})

	t.Run("returns the patch error", func(t *testing.T) {
		cell := stampedCell(nil)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, cell).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(
					ctx context.Context,
					cl client.WithWatch,
					obj client.Object,
					patch client.Patch,
					opts ...client.PatchOption,
				) error {
					if patch.Type() == types.MergePatchType {
						return errors.New("boom")
					}
					return cl.Patch(ctx, obj, patch, opts...)
				},
			}).Build()
		if _, err := reconcile(t, c); err == nil {
			t.Fatal("expected the clear to fail")
		}
	})
}

func TestReconcileCells_DeletesOnlyTheVersionItJudgedReady(t *testing.T) {
	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd"},
			},
		},
	}
	ready := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-zone-old",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test"},
			Annotations: map[string]string{
				multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
			},
		},
		Spec: multigresv1alpha1.CellSpec{Name: "zone-old"},
		Status: multigresv1alpha1.CellStatus{
			Conditions: []metav1.Condition{{
				Type:               multigresv1alpha1.ConditionReadyForDeletion,
				Status:             metav1.ConditionTrue,
				Reason:             "StatelessComponent",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}

	var gotPreconditions *metav1.Preconditions
	deleteErr := apierrors.NewConflict(
		schema.GroupResource{Group: "multigres.com", Resource: "cells"},
		ready.Name, errors.New("resourceVersion changed"))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, ready).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(
				_ context.Context,
				_ client.WithWatch,
				_ client.Object,
				opts ...client.DeleteOption,
			) error {
				do := &client.DeleteOptions{}
				do.ApplyOptions(opts)
				gotPreconditions = do.Preconditions
				return deleteErr
			},
		}).Build()
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	listed := &multigresv1alpha1.Cell{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ready), listed); err != nil {
		t.Fatal(err)
	}
	pending, err := r.reconcileCells(
		context.Background(),
		cluster,
		resolver.NewResolver(c, "default"),
	)
	if err != nil {
		t.Fatalf("a conflicting delete is a retry, not an error: %v", err)
	}
	if !pending {
		t.Error("a Cell whose delete conflicted is still pending deletion")
	}
	if gotPreconditions == nil || gotPreconditions.UID == nil ||
		gotPreconditions.ResourceVersion == nil {
		t.Fatalf("delete carried no UID and resourceVersion precondition: %+v", gotPreconditions)
	}
	if *gotPreconditions.UID != listed.UID ||
		*gotPreconditions.ResourceVersion != listed.ResourceVersion {
		t.Errorf("precondition %s@%s, want the version read: %s@%s",
			*gotPreconditions.UID, *gotPreconditions.ResourceVersion,
			listed.UID, listed.ResourceVersion)
	}
}
