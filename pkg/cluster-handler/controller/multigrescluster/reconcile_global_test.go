package multigrescluster

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

func TestReconcileGlobal_ErrorPaths(t *testing.T) {
	scheme := setupScheme()

	t.Run("Error: Resolve Global Topo Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					TemplateRef: "non-existent-core",
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileGlobalTopoServer(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil {
			t.Error("Expected error due to missing global topo spec, got nil")
		}
	})

	t.Run("Error: Resolve Multiadmin Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Multiadmin: &multigresv1alpha1.MultiadminConfig{
					TemplateRef: "non-existent-core",
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileMultiadmin(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil {
			t.Error("Expected error due to missing multi admin spec, got nil")
		}
	})

	t.Run("Error: Resolve MultiadminWeb Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				MultiadminWeb: &multigresv1alpha1.MultiadminWebConfig{
					TemplateRef: "non-existent-core",
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileMultiadminWeb(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil {
			t.Error("Expected error due to missing multi admin web spec, got nil")
		}
	})

	t.Run("Error: Patch Global Topo Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd"},
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
		err := r.reconcileGlobalTopoServer(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil || err.Error() != "failed to apply global topo server: patch error" {
			t.Errorf("Expected 'patch error', got %v", err)
		}
	})

	t.Run("Error: Patch Multiadmin Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			// Multiadmin is created by default unless disabled, so it should try to patch.
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
		err := r.reconcileMultiadmin(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil || err.Error() != "failed to apply multiadmin deployment: patch error" {
			t.Errorf("Expected 'patch error', got %v", err)
		}
	})

	t.Run("Error: Patch MultiadminWeb Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			// MultiadminWeb also defaults to enabled if image is present (which defaults handles)
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
		err := r.reconcileMultiadminWeb(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil || err.Error() != "failed to apply multiadmin-web deployment: patch error" {
			t.Errorf("Expected 'patch error', got %v", err)
		}
	})

	t.Run("Error: Build Global Topo Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd"},
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		// Use empty scheme for Reconciler
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   runtime.NewScheme(),
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileGlobalTopoServer(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil {
			t.Error("Expected error due to build failure, got nil")
		}
	})

	t.Run("Error: Build Multiadmin Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			// Multiadmin default enabled
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		// Use empty scheme for Reconciler
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   runtime.NewScheme(),
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileMultiadmin(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil {
			t.Error("Expected error due to build failure, got nil")
		}
	})

	t.Run("Error: Build MultiadminWeb Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			// MultiadminWeb default enabled
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		// Use empty scheme for Reconciler
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   runtime.NewScheme(),
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileMultiadminWeb(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil {
			t.Error("Expected error due to build failure, got nil")
		}
	})

	t.Run("Error: Build Multiadmin Service Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		}

		s := runtime.NewScheme()
		_ = appsv1.AddToScheme(s)
		_ = multigresv1alpha1.AddToScheme(s) // Required for SetControllerReference

		// Use limited scheme for Client so Patch fails or Build fails if it uses client scheme?
		// Build uses r.Scheme.
		// Patch uses r.Client which uses client scheme.
		// If we want Patch to fail due to missing kind, client scheme must miss it.
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build()

		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   s,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileMultiadmin(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil {
			t.Error("Expected error due to build failure, got nil")
		}
	})

	t.Run("Error: Patch Multiadmin Service Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					return errors.New("service patch error")
				}
				return cli.Patch(ctx, obj, patch, opts...)
			},
		}).WithObjects(cluster).Build()

		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileMultiadmin(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil || err.Error() != "failed to apply multiadmin service: service patch error" {
			t.Errorf("Expected 'service patch error', got %v", err)
		}
	})

	t.Run("Error: Build MultiadminWeb Service Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		}

		s := runtime.NewScheme()
		_ = appsv1.AddToScheme(s)
		_ = multigresv1alpha1.AddToScheme(s)

		c := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   s,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileMultiadminWeb(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil {
			t.Error("Expected error due to build failure, got nil")
		}
	})

	t.Run("Error: Patch MultiadminWeb Service Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					return errors.New("service patch error")
				}
				return cli.Patch(ctx, obj, patch, opts...)
			},
		}).WithObjects(cluster).Build()

		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileMultiadminWeb(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil ||
			err.Error() != "failed to apply multiadmin-web service: service patch error" {
			t.Errorf("Expected 'service patch error', got %v", err)
		}
	})

	t.Run("Error: Patch Multigateway Global Service Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		}

		patchCount := 0
		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					patchCount++
					// First Service patch is multiadmin-web, second is multigateway
					if patchCount == 2 {
						return errors.New("gw service patch error")
					}
				}
				return cli.Patch(ctx, obj, patch, opts...)
			},
		}).WithObjects(cluster).Build()

		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileMultiadminWeb(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil ||
			err.Error() != "failed to apply global multigateway service: gw service patch error" {
			t.Errorf("Expected 'gw service patch error', got %v", err)
		}
	})

	t.Run("Error: Patch Multigateway Global Replica Service Failed", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if svc, ok := obj.(*corev1.Service); ok &&
					svc.Name == "test-multigateway-replica" {
					return errors.New("gw replica service patch error")
				}
				return cli.Patch(ctx, obj, patch, opts...)
			},
		}).WithObjects(cluster).Build()

		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.reconcileMultiadminWeb(
			context.Background(),
			cluster,
			resolver.NewResolver(c, "default"),
		)
		if err == nil ||
			err.Error() != "failed to apply global replica multigateway service: gw replica service patch error" {
			t.Errorf("Expected 'gw replica service patch error', got %v", err)
		}
	})
}

func TestReconcile_Global(t *testing.T) {
	coreTpl, cellTpl, shardTpl, _, clusterName, namespace := setupFixtures(t)
	errSimulated := errors.New("simulated error for testing")

	tests := map[string]reconcileTestCase{
		"Create: Independent Templates (Topo vs Admin)": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.TemplateDefaults.CoreTemplate = "" // clear default
				c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
					TemplateRef: "topo-core",
				}
				c.Spec.Multiadmin = &multigresv1alpha1.MultiadminConfig{
					TemplateRef: "admin-core",
				}
			},
			existingObjects: []client.Object{
				cellTpl, shardTpl,
				&multigresv1alpha1.CoreTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "topo-core", Namespace: namespace},
					Spec: multigresv1alpha1.CoreTemplateSpec{
						GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
							Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd:topo"},
						},
					},
				},
				&multigresv1alpha1.CoreTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "admin-core", Namespace: namespace},
					Spec: multigresv1alpha1.CoreTemplateSpec{
						Multiadmin: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(5))},
					},
				},
			},
			validate: func(t testing.TB, c client.Client) {
				ctx := t.Context()

				ts := &multigresv1alpha1.TopoServer{}
				if err := c.Get(
					ctx,
					types.NamespacedName{Name: clusterName + "-global-topo", Namespace: namespace},
					ts,
				); err != nil {
					t.Fatal(err)
				}
				if got, want := ts.Spec.Etcd.Image, multigresv1alpha1.ImageRef(
					"etcd:topo",
				); got != want {
					t.Errorf("TopoServer image mismatch got %q, want %q", got, want)
				}

				deploy := &appsv1.Deployment{}
				if err := c.Get(
					ctx,
					types.NamespacedName{Name: clusterName + "-multiadmin", Namespace: namespace},
					deploy,
				); err != nil {
					t.Fatal(err)
				}
				if got, want := *deploy.Spec.Replicas, int32(5); got != want {
					t.Errorf("Multiadmin replicas mismatch got %d, want %d", got, want)
				}

				webDeploy := &appsv1.Deployment{}
				if err := c.Get(
					ctx,
					types.NamespacedName{
						Name:      clusterName + "-multiadmin-web",
						Namespace: namespace,
					},
					webDeploy,
				); err != nil {
					t.Fatal(err)
				}
				// Default replicas is 1
				if got, want := *webDeploy.Spec.Replicas, int32(1); got != want {
					t.Errorf("MultiadminWeb replicas mismatch got %d, want %d", got, want)
				}

				// Verify global multigateway Service exists
				gwSvc := &corev1.Service{}
				if err := c.Get(
					ctx,
					types.NamespacedName{Name: clusterName + "-multigateway", Namespace: namespace},
					gwSvc,
				); err != nil {
					t.Fatalf("Expected global multigateway Service to exist: %v", err)
				}
				if gwSvc.Spec.Selector["app.kubernetes.io/component"] != "multigateway" {
					t.Errorf(
						"Global multigateway Service selector component = %v, want multigateway",
						gwSvc.Spec.Selector["app.kubernetes.io/component"],
					)
				}

				// Verify global replica multigateway Service exists
				gwReplicaSvc := &corev1.Service{}
				if err := c.Get(
					ctx,
					types.NamespacedName{
						Name:      clusterName + "-multigateway-replica",
						Namespace: namespace,
					},
					gwReplicaSvc,
				); err != nil {
					t.Fatalf("Expected global multigateway replica Service to exist: %v", err)
				}
				if len(gwReplicaSvc.Spec.Ports) != 1 ||
					gwReplicaSvc.Spec.Ports[0].Port != 5433 ||
					gwReplicaSvc.Spec.Ports[0].TargetPort != intstr.FromString("pg-replica") {
					t.Errorf(
						"Global multigateway replica Service ports mismatch: %+v",
						gwReplicaSvc.Spec.Ports,
					)
				}
			},
		},

		"Create: Managed Etcd with RootPath": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Image:    "etcd:test",
						RootPath: "/custom/root",
					},
				}
			},
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			validate: func(t testing.TB, c client.Client) {
				ctx := t.Context()
				ts := &multigresv1alpha1.TopoServer{}
				if err := c.Get(
					ctx,
					types.NamespacedName{Name: clusterName + "-global-topo", Namespace: namespace},
					ts,
				); err != nil {
					t.Fatal(err)
				}
				if got, want := ts.Spec.Etcd.RootPath, "/custom/root"; got != want {
					t.Errorf("RootPath mismatch got %q, want %q", got, want)
				}
			},
		},

		"Create: External Topo Integration": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
					External: &multigresv1alpha1.ExternalTopoServerSpec{
						Endpoints: []multigresv1alpha1.EndpointUrl{"http://external-etcd:2379"},
					},
				}
			},
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			validate: func(t testing.TB, c client.Client) {
				ctx := t.Context()
				ts := &multigresv1alpha1.TopoServer{}
				if err := c.Get(
					ctx,
					types.NamespacedName{Name: clusterName + "-global-topo", Namespace: namespace},
					ts,
				); !apierrors.IsNotFound(
					err,
				) {
					t.Fatal("Global TopoServer should NOT be created for External mode")
				}
				cell := &multigresv1alpha1.Cell{}
				// Use hashed name for Cell
				cellName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					"zone-a",
				)
				if err := c.Get(
					ctx,
					types.NamespacedName{Name: cellName, Namespace: namespace},
					cell,
				); err != nil {
					t.Fatalf("Expected Cell %s to exist: %v", cellName, err)
				}
				if got, want := cell.Spec.GlobalTopoServer.Address, "http://external-etcd:2379"; got != want {
					t.Errorf("External address mismatch got %q, want %q", got, want)
				}
			},
		},
		"Error: Explicit Core Template Missing (Should Fail)": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.TemplateDefaults.CoreTemplate = "non-existent-template"
			},
			existingObjects: []client.Object{}, // No templates exist
			failureConfig:   nil,
			wantErrMsg:      "failed to resolve global topo",
		},
		"Error: Resolve CoreTemplate Failed": {
			existingObjects: []client.Object{coreTpl},
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnKeyName("default-core", errSimulated),
			},
			wantErrMsg: "failed to resolve global topo",
		},
		"Error: Resolve Admin Template Failed (Second Call)": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.TemplateDefaults.CoreTemplate = ""
				c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
					TemplateRef: "topo-core",
				}
				c.Spec.Multiadmin = &multigresv1alpha1.MultiadminConfig{
					TemplateRef: "admin-core-fail",
				}
			},
			existingObjects: []client.Object{
				cellTpl, shardTpl,
				&multigresv1alpha1.CoreTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "topo-core", Namespace: namespace},
					Spec:       multigresv1alpha1.CoreTemplateSpec{},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnKeyName("admin-core-fail", errSimulated),
			},
			wantErrMsg: "failed to resolve multiadmin",
		},
		"Error: Apply GlobalTopo Failed": {
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(clusterName+"-global-topo", errSimulated),
			},
			wantErrMsg: "failed to apply global topo server",
		},

		"Error: Apply Multiadmin Failed": {
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(clusterName+"-multiadmin", errSimulated),
			},
			wantErrMsg: "failed to apply multiadmin deployment",
		},
		"Update: Global Components Success": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "new-etcd"},
				}
				c.Spec.Multiadmin = &multigresv1alpha1.MultiadminConfig{
					Spec: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(3))},
				}
				c.Spec.MultiadminWeb = &multigresv1alpha1.MultiadminWebConfig{
					Spec: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(2))},
				}
			},
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				&multigresv1alpha1.TopoServer{
					ObjectMeta: metav1.ObjectMeta{
						Name:      clusterName + "-global-topo",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
					Spec: multigresv1alpha1.TopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{Image: "old-etcd"},
					},
				},
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      clusterName + "-multiadmin",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
					},
				},
			},
			validate: func(t testing.TB, c client.Client) {
				ts := &multigresv1alpha1.TopoServer{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: clusterName + "-global-topo", Namespace: namespace},
					ts,
				); err != nil {
					t.Fatal(err)
				}
				if ts.Spec.Etcd.Image != "new-etcd" {
					t.Errorf("TopoServer not updated")
				}
				deploy := &appsv1.Deployment{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: clusterName + "-multiadmin", Namespace: namespace},
					deploy,
				); err != nil {
					t.Fatal(err)
				}
				if *deploy.Spec.Replicas != 3 {
					t.Errorf("Multiadmin not updated")
				}

				webDeploy := &appsv1.Deployment{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{
						Name:      clusterName + "-multiadmin-web",
						Namespace: namespace,
					},
					webDeploy,
				); err != nil {
					t.Fatal(err)
				}
				if *webDeploy.Spec.Replicas != 2 {
					t.Errorf("MultiadminWeb not updated")
				}
			},
		},
		"Idempotency: No changes needed": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				// No changes to spec
			},
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				&multigresv1alpha1.TopoServer{
					ObjectMeta: metav1.ObjectMeta{
						Name:      clusterName + "-global-topo",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
					Spec: multigresv1alpha1.TopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd:topo"},
					},
				},
				// We need to ensure we have the other objects too otherwise they will be created
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      clusterName + "-multiadmin",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(5)), // Matches admin-core which has 5 replicas
					},
				},
				// MultiadminWeb will participate
			},
			validate: func(t testing.TB, c client.Client) {
				// Just ensure no error
			},
		},
		"Error: Reconcile Global Topo Failed (Propagated)": {
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(clusterName+"-global-topo", errSimulated),
			},
			wantErrMsg: "simulated error",
		},
		"Error: Reconcile Multiadmin Failed (Propagated)": {
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(clusterName+"-multiadmin", errSimulated),
			},
			wantErrMsg: "simulated error",
		},
		"Error: Reconcile MultiadminWeb Failed (Propagated)": {
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(clusterName+"-multiadmin-web", errSimulated),
			},
			wantErrMsg: "simulated error",
		},
		"Error: Reconcile Multiadmin Service Failed": {
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if obj.GetName() == clusterName+"-multiadmin" &&
						obj.GetObjectKind().GroupVersionKind().Kind == "Service" {
						return errSimulated
					}
					return nil
				},
			},
			wantErrMsg: "failed to apply multiadmin service",
		},
		"Error: Reconcile MultiadminWeb Service Failed": {
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if obj.GetName() == clusterName+"-multiadmin-web" &&
						obj.GetObjectKind().GroupVersionKind().Kind == "Service" {
						return errSimulated
					}
					return nil
				},
			},
			wantErrMsg: "failed to apply multiadmin-web service",
		},
		"Error: Reconcile Global Multigateway Service Failed": {
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if obj.GetName() == clusterName+"-multigateway" &&
						obj.GetObjectKind().GroupVersionKind().Kind == "Service" {
						return errSimulated
					}
					return nil
				},
			},
			wantErrMsg: "failed to apply global multigateway service",
		},
		"Error: Reconcile Global Multigateway Replica Service Failed": {
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if obj.GetName() == clusterName+"-multigateway-replica" &&
						obj.GetObjectKind().GroupVersionKind().Kind == "Service" {
						return errSimulated
					}
					return nil
				},
			},
			wantErrMsg: "failed to apply global replica multigateway service",
		},
		"Error: Build Multiadmin Deployment Failed (Scheme)": {
			// Bypass Global Topo builder by using External spec
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
					External: &multigresv1alpha1.ExternalTopoServerSpec{
						Endpoints: []multigresv1alpha1.EndpointUrl{"http://external:2379"},
					},
				}
			},
			// Inject a scheme that does NOT have the MultigresCluster type registered.
			customReconcilerScheme: func() *runtime.Scheme {
				s := runtime.NewScheme()
				_ = appsv1.AddToScheme(s) // Needed for Deployment
				_ = corev1.AddToScheme(s) // Needed for Service
				return s
			}(),
			wantErrMsg: "failed to build multiadmin deployment",
		},
		"Error: Build Global Topo Failed (Scheme)": {
			customReconcilerScheme: func() *runtime.Scheme {
				s := runtime.NewScheme()
				_ = appsv1.AddToScheme(s)
				_ = corev1.AddToScheme(s)
				// Missing MultigresCluster
				return s
			}(),
			wantErrMsg: "failed to build global topo server",
		},
	}

	runReconcileTest(t, tests)
}

// TestReconcile_Global_BuilderErrors tests builder errors that are difficult to reach via integration schemes
// due to sequential dependencies. We use variable indirection to mock the builder functions.
func TestReconcile_Global_BuilderErrors(t *testing.T) {
	// This test modifies package-level variables, so it cannot run in parallel.
	// t.Parallel() is intentionally omitted.

	scheme := setupScheme()
	coreTpl, _, _, baseCluster, _, _ := setupFixtures(t)

	t.Run("Error: Build Multiadmin Service Failed (Mock)", func(t *testing.T) {
		// Mock the builder
		originalBuild := buildMultiadminService
		defer func() { buildMultiadminService = originalBuild }()

		buildMultiadminService = func(_ *multigresv1alpha1.MultigresCluster, _ *runtime.Scheme) (*corev1.Service, error) {
			return nil, errors.New("mocked builder error")
		}

		// Setup client
		clientBuilder := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{})
		c := clientBuilder.Build()

		// Manually create template to ensure it exists
		if err := c.Create(t.Context(), coreTpl.DeepCopy()); err != nil {
			t.Fatalf("Failed to create CoreTemplate: %v", err)
		}

		reconciler := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		// Use External Topo to skip Topo builder failure
		cluster := baseCluster.DeepCopy()
		cluster.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
			External: &multigresv1alpha1.ExternalTopoServerSpec{
				Endpoints: []multigresv1alpha1.EndpointUrl{"http://external:2379"},
			},
		}

		// Initialize resolver with correct namespace and defaults using constructor
		res := resolver.NewResolver(c, baseCluster.Namespace)
		err := reconciler.reconcileMultiadmin(t.Context(), cluster, res)
		if err == nil {
			t.Error("Expected error, got nil")
		} else if !strings.Contains(err.Error(), "failed to build multiadmin service") {
			t.Errorf("Expected 'failed to build multiadmin service', got: %v", err)
		}
	})

	t.Run("Error: Build MultiadminWeb Deployment Failed (Mock)", func(t *testing.T) {
		// Mock the builder
		originalBuild := buildMultiadminWebDeployment
		defer func() { buildMultiadminWebDeployment = originalBuild }()

		buildMultiadminWebDeployment = func(_ *multigresv1alpha1.MultigresCluster, _ *multigresv1alpha1.StatelessSpec, _ *runtime.Scheme) (*appsv1.Deployment, error) {
			return nil, errors.New("mocked builder error")
		}

		// Setup client
		clientBuilder := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{})
		c := clientBuilder.Build()

		// Manually create template to ensure it exists
		if err := c.Create(t.Context(), coreTpl.DeepCopy()); err != nil {
			t.Fatalf("Failed to create CoreTemplate: %v", err)
		}

		reconciler := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		cluster := baseCluster.DeepCopy()
		res := resolver.NewResolver(c, baseCluster.Namespace)
		err := reconciler.reconcileMultiadminWeb(t.Context(), cluster, res)
		if err == nil {
			t.Error("Expected error, got nil")
		} else if !strings.Contains(err.Error(), "failed to build multiadmin-web deployment") {
			t.Errorf("Expected 'failed to build multiadmin-web deployment', got: %v", err)
		}
	})

	t.Run("Error: Build MultiadminWeb Service Failed (Mock)", func(t *testing.T) {
		// Mock the builder
		originalBuild := buildMultiadminWebService
		defer func() { buildMultiadminWebService = originalBuild }()

		buildMultiadminWebService = func(_ *multigresv1alpha1.MultigresCluster, _ *multigresv1alpha1.ExternalAdminWebConfig, _ *runtime.Scheme) (*corev1.Service, error) {
			return nil, errors.New("mocked builder error")
		}

		// Setup client
		clientBuilder := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{})
		c := clientBuilder.Build()

		// Manually create template to ensure it exists
		if err := c.Create(t.Context(), coreTpl.DeepCopy()); err != nil {
			t.Fatalf("Failed to create CoreTemplate: %v", err)
		}

		reconciler := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		cluster := baseCluster.DeepCopy()
		res := resolver.NewResolver(c, baseCluster.Namespace)
		err := reconciler.reconcileMultiadminWeb(t.Context(), cluster, res)
		if err == nil {
			t.Error("Expected error, got nil")
		} else if !strings.Contains(err.Error(), "failed to build multiadmin-web service") {
			t.Errorf("Expected 'failed to build multiadmin-web service', got: %v", err)
		}
	})

	t.Run("Error: Build Multigateway Global Service Failed (Mock)", func(t *testing.T) {
		originalBuild := buildMultigatewayGlobalService
		defer func() { buildMultigatewayGlobalService = originalBuild }()

		buildMultigatewayGlobalService = func(_ *multigresv1alpha1.MultigresCluster, _ *multigresv1alpha1.ExternalGatewayConfig, _ *runtime.Scheme) (*corev1.Service, error) {
			return nil, errors.New("mocked builder error")
		}

		clientBuilder := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{})
		c := clientBuilder.Build()

		if err := c.Create(t.Context(), coreTpl.DeepCopy()); err != nil {
			t.Fatalf("Failed to create CoreTemplate: %v", err)
		}

		reconciler := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		cluster := baseCluster.DeepCopy()
		res := resolver.NewResolver(c, baseCluster.Namespace)
		err := reconciler.reconcileMultiadminWeb(t.Context(), cluster, res)
		if err == nil {
			t.Error("Expected error, got nil")
		} else if !strings.Contains(err.Error(), "failed to build global multigateway service") {
			t.Errorf("Expected 'failed to build global multigateway service', got: %v", err)
		}
	})

	t.Run("Error: Build Multigateway Global Replica Service Failed (Mock)", func(t *testing.T) {
		originalBuild := buildMultigatewayGlobalReplicaService
		defer func() { buildMultigatewayGlobalReplicaService = originalBuild }()

		buildMultigatewayGlobalReplicaService = func(_ *multigresv1alpha1.MultigresCluster, _ *runtime.Scheme) (*corev1.Service, error) {
			return nil, errors.New("mocked builder error")
		}

		clientBuilder := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{})
		c := clientBuilder.Build()

		if err := c.Create(t.Context(), coreTpl.DeepCopy()); err != nil {
			t.Fatalf("Failed to create CoreTemplate: %v", err)
		}

		reconciler := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		cluster := baseCluster.DeepCopy()
		res := resolver.NewResolver(c, baseCluster.Namespace)
		err := reconciler.reconcileMultiadminWeb(t.Context(), cluster, res)
		if err == nil {
			t.Error("Expected error, got nil")
		} else if !strings.Contains(err.Error(), "failed to build global replica multigateway service") {
			t.Errorf("Expected 'failed to build global replica multigateway service', got: %v", err)
		}
	})
}
