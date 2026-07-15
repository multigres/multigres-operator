package multigrescluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
)

// ============================================================================
// Shared Test Types & Helpers
// ============================================================================

// reconcileTestCase defines the structure for all controller unit tests
type reconcileTestCase struct {
	multigrescluster    *multigresv1alpha1.MultigresCluster
	existingObjects     []client.Object
	failureConfig       *testutil.FailureConfig
	preReconcileUpdate  func(testing.TB, *multigresv1alpha1.MultigresCluster)
	skipClusterCreation bool
	wantErrMsg          string
	// NEW: Verify specific events were emitted
	expectedEvents []string
	validate       func(testing.TB, client.Client)

	// NEW: Allow injecting a different scheme for the Reconciler (to test builder errors)
	customReconcilerScheme *runtime.Scheme
}

// runReconcileTest is the shared runner for all split test files
func runReconcileTest(t *testing.T, tests map[string]reconcileTestCase) {
	t.Helper()

	scheme := setupScheme()

	coreTpl, cellTpl, shardTpl, baseCluster, _, _ := setupFixtures(t)

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Default to all standard templates if existingObjects is nil
			objects := tc.existingObjects
			if objects == nil {
				objects = []client.Object{coreTpl, cellTpl, shardTpl}
			}

			clientBuilder := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}, &multigresv1alpha1.Cell{}, &multigresv1alpha1.TableGroup{})
			baseClient := clientBuilder.Build()

			var finalClient client.Client
			finalClient = client.Client(baseClient)
			if tc.failureConfig != nil {
				finalClient = testutil.NewFakeClientWithFailures(baseClient, tc.failureConfig)
			}

			// Apply defaults if no specific cluster is provided
			cluster := tc.multigrescluster
			if cluster == nil {
				cluster = baseCluster.DeepCopy()
			}

			// Apply pre-reconcile updates if defined
			if tc.preReconcileUpdate != nil {
				tc.preReconcileUpdate(t, cluster)
			}

			shouldDelete := cluster.GetDeletionTimestamp() != nil &&
				!cluster.GetDeletionTimestamp().IsZero()

			if !strings.Contains(name, "Object Not Found") {
				check := &multigresv1alpha1.MultigresCluster{}
				err := baseClient.Get(
					t.Context(),
					types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
					check,
				)
				if apierrors.IsNotFound(err) {
					if err := baseClient.Create(t.Context(), cluster); err != nil {
						t.Fatalf("failed to create initial cluster: %v", err)
					}

					if shouldDelete {
						// Simulate deletion workflow
						if err := baseClient.Get(
							t.Context(),
							types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
							cluster,
						); err != nil {
							t.Fatalf("failed to refresh cluster before delete: %v", err)
						}
						if err := baseClient.Delete(t.Context(), cluster); err != nil {
							t.Fatalf("failed to set deletion timestamp: %v", err)
						}
						if err := baseClient.Get(
							t.Context(),
							types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
							cluster,
						); err != nil {
							t.Fatalf("failed to refresh cluster after deletion: %v", err)
						}
					}
				}
			}

			// Create a buffered fake recorder to capture events
			fakeRecorder := record.NewFakeRecorder(100)

			// Determine scheme for Reconciler
			reconcilerScheme := scheme
			if tc.customReconcilerScheme != nil {
				reconcilerScheme = tc.customReconcilerScheme
			}

			reconciler := &MultigresClusterReconciler{
				Client:   finalClient,
				Scheme:   reconcilerScheme,
				Recorder: fakeRecorder,
				CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
					_, factory := memorytopo.NewServerAndFactory(t.Context())
					store := topoclient.NewWithFactory(
						factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
					)
					return store, nil
				},
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      cluster.Name,
					Namespace: cluster.Namespace,
				},
			}

			result, err := reconciler.Reconcile(t.Context(), req)
			t.Logf("DEBUG: Reconcile result=%+v err=%v", result, err)

			if tc.wantErrMsg != "" {
				if err == nil {
					t.Error("Expected error from Reconcile, got nil")
				} else if !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf(
						"Error mismatch. Expected substring %q, got %q",
						tc.wantErrMsg,
						err.Error(),
					)
				}
			} else if err != nil {
				t.Errorf("Unexpected error from Reconcile: %v", err)
			}

			// Verify Events
			if len(tc.expectedEvents) > 0 {
				close(fakeRecorder.Events)
				var gotEvents []string
				for evt := range fakeRecorder.Events {
					gotEvents = append(gotEvents, evt)
				}

				for _, want := range tc.expectedEvents {
					found := false
					for _, got := range gotEvents {
						if strings.Contains(got, want) {
							found = true
							break
						}
					}
					if !found {
						t.Errorf(
							"Expected event containing %q not found. Got events: %v",
							want,
							gotEvents,
						)
					}
				}
			}

			if tc.validate != nil {
				tc.validate(t, baseClient)
			}
		})
	}
}

// setupScheme creates a new scheme with all required types registered
func setupScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	// Register cert-manager Certificate so the fake client does not
	// mutate the scheme at runtime (which panics under parallel tests).
	registerCertManagerTypes(scheme)
	return scheme
}

// setupFixtures provides fresh test data
func setupFixtures(tb testing.TB) (
	*multigresv1alpha1.CoreTemplate,
	*multigresv1alpha1.CellTemplate,
	*multigresv1alpha1.ShardTemplate,
	*multigresv1alpha1.MultigresCluster,
	string, string,
) {
	tb.Helper()

	clusterName := "test-cluster"
	namespace := "default"

	coreTpl := &multigresv1alpha1.CoreTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default-core", Namespace: namespace},
		Spec: multigresv1alpha1.CoreTemplateSpec{
			GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:    "etcd:v1",
					Replicas: ptr.To(int32(3)),
				},
			},
			Multiadmin: &multigresv1alpha1.StatelessSpec{
				Replicas: ptr.To(int32(1)),
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("100m")},
				},
			},
		},
	}
	coreTpl.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("CoreTemplate"))

	cellTpl := &multigresv1alpha1.CellTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default-cell", Namespace: namespace},
		Spec: multigresv1alpha1.CellTemplateSpec{
			Multigateway: &multigresv1alpha1.StatelessSpec{
				Replicas: ptr.To(int32(2)),
			},
		},
	}
	cellTpl.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("CellTemplate"))

	shardTpl := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default-shard", Namespace: namespace},
		Spec: multigresv1alpha1.ShardTemplateSpec{
			Multiorch: &multigresv1alpha1.MultiorchSpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{
					Replicas: ptr.To(int32(3)),
				},
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					ReplicasPerCell: ptr.To(int32(2)),
					Type:            "readWrite",
				},
			},
		},
	}
	shardTpl.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("ShardTemplate"))

	baseCluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
			Labels: map[string]string{
				metadata.LabelUsesCoreTemplate:  "true",
				metadata.LabelUsesCellTemplate:  "true",
				metadata.LabelUsesShardTemplate: "true",
			},
			// Pre-set CreationTimestamp so the fake client preserves it on Create.
			// This bypasses the 2-minute topology startup grace period in reconcileTopology.
			CreationTimestamp: metav1.NewTime(time.Now().Add(-1 * time.Hour)),
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Images: multigresv1alpha1.ClusterImages{
				Multigateway:     "gateway:latest",
				Multiorch:        "orch:latest",
				Multipooler:      "pooler:latest",
				Multiadmin:       "admin:latest",
				Postgres:         "postgres:15",
				ImagePullPolicy:  corev1.PullAlways,
				ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
			},
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate:  "default-core",
				CellTemplate:  "default-cell",
				ShardTemplate: "default-shard",
			},
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				TemplateRef: "default-core",
			},
			Multiadmin: &multigresv1alpha1.MultiadminConfig{
				TemplateRef: "default-core",
			},
			Cells: []multigresv1alpha1.CellConfig{
				{Name: "zone-a", ZoneID: "use1-az1"},
			},
			Databases: []multigresv1alpha1.DatabaseConfig{
				{
					Name: "db1",
					TableGroups: []multigresv1alpha1.TableGroupConfig{
						{Name: "tg1", Shards: []multigresv1alpha1.ShardConfig{{Name: "s1"}}},
					},
				},
			},
		},
	}

	return coreTpl, cellTpl, shardTpl, baseCluster, clusterName, namespace
}

func parseQty(s string) resource.Quantity {
	return resource.MustParse(s)
}

// ============================================================================
// Main Controller Logic & Lifecycle Tests
// ============================================================================

func TestMultigresClusterReconciler_Lifecycle(t *testing.T) {
	// Enable W3C trace context propagation so ExtractTraceContext can parse traceparent annotations.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	coreTpl, cellTpl, shardTpl, _, clusterName, namespace := setupFixtures(t)
	errSimulated := errors.New("simulated error for testing")

	tests := map[string]reconcileTestCase{
		"Create: Full Cluster Creation - Verify Images and Wiring": {
			expectedEvents: []string{"Normal Synced Successfully reconciled MultigresCluster"},
			validate: func(t testing.TB, c client.Client) {
				ctx := t.Context()
				// Verify Cell (Basic wiring check)
				cell := &multigresv1alpha1.Cell{}
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
				if got, want := cell.Spec.Images.Multigateway, multigresv1alpha1.ImageRef(
					"gateway:latest",
				); got != want {
					t.Errorf("Cell image mismatch got %q, want %q", got, want)
				}
			},
		},

		"Error: Fetch Cluster Failed": {
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnKeyName(clusterName, errSimulated),
			},
			wantErrMsg: "failed to get MultigresCluster",
		},

		"Success: Trigger Implicit Defaults": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.TemplateDefaults = multigresv1alpha1.TemplateDefaults{} // Empty defaults to trigger population
			},
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			expectedEvents: []string{
				"Normal ImplicitDefault",
				"Normal Synced Successfully reconciled MultigresCluster",
			},
		},

		"Success: TableGroup Name Too Long (Hashed)": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases = []multigresv1alpha1.DatabaseConfig{
					{
						Name: "db1",
						TableGroups: []multigresv1alpha1.TableGroupConfig{
							{
								Name:   "this-name-is-extremely-long-and-will-fail-validation",
								Shards: []multigresv1alpha1.ShardConfig{{Name: "s1"}},
							},
						},
					},
				}
			},
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			expectedEvents:  []string{"Normal Synced Successfully reconciled MultigresCluster"},
			wantErrMsg:      "",
		},
		"Error: Apply TableGroup Failed": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(
					name.JoinWithConstraints(name.DefaultConstraints, clusterName, "db1", "tg1"),
					errSimulated,
				),
			},
			expectedEvents: []string{"Warning FailedApply Failed to reconcile databases"},
			wantErrMsg:     "failed to apply tablegroup",
		},
		"Error: Set PendingDeletion on Orphan TableGroup Failed": {
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				&multigresv1alpha1.TableGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							"db1",
							"orphan-tg",
						),
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(
					name.JoinWithConstraints(
						name.DefaultConstraints,
						clusterName,
						"db1",
						"orphan-tg",
					),
					errSimulated,
				),
			},
			expectedEvents: []string{"Warning FailedApply Failed to reconcile databases"},
			wantErrMsg:     "failed to set PendingDeletion on tablegroup",
		},
		"Success: Mark Orphan TableGroup PendingDeletion": {
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				&multigresv1alpha1.TableGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      clusterName + "-orphan-tg",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
				},
			},
			expectedEvents: []string{
				"Normal PendingDeletion Marked TableGroup",
			},
			validate: func(t testing.TB, c client.Client) {
				tg := &multigresv1alpha1.TableGroup{}
				err := c.Get(
					t.Context(),
					types.NamespacedName{Name: clusterName + "-orphan-tg", Namespace: namespace},
					tg,
				)
				if err != nil {
					t.Fatalf("Expected orphan TG to still exist with PendingDeletion, got: %v", err)
				}
				if tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
					t.Error("Expected orphan TableGroup to have PendingDeletion annotation")
				}
			},
		},
		"Object Not Found (Clean Exit)": {
			skipClusterCreation: true,
			existingObjects:     []client.Object{},
		},
		"Error: PopulateClusterDefaults Failed (Implicit Shard Check)": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.TemplateDefaults.ShardTemplate = "" // Force implicit check
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnNamespacedKeyName("default", namespace, errSimulated),
			},
			wantErrMsg: "failed to check for implicit shard template",
		},
		"Error: Reconcile Global Components Failed": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(clusterName+"-global-topo", errSimulated),
			},
			expectedEvents: []string{"Warning FailedApply Failed to reconcile global components"},
			wantErrMsg:     "failed to apply global topo server",
		},
		"Error: Reconcile Cells Failed": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(
					name.JoinWithConstraints(name.DefaultConstraints, clusterName, "zone-a"),
					errSimulated,
				),
			},
			expectedEvents: []string{"Warning FailedApply Failed to reconcile cells"},
			wantErrMsg:     "failed to apply cell",
		},
		"Error: Reconcile Databases Failed": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(
					name.JoinWithConstraints(name.DefaultConstraints, clusterName, "db1", "tg1"),
					errSimulated,
				),
			},
			expectedEvents: []string{"Warning FailedApply Failed to reconcile databases"},
			wantErrMsg:     "failed to apply tablegroup",
		},
		"Error: Update Status Failed": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			failureConfig: &testutil.FailureConfig{
				OnStatusPatch: func(obj client.Object) error {
					if obj.GetName() != clusterName {
						return fmt.Errorf(
							"OnStatusPatch called for wrong object: '%s' (wanted '%s')",
							obj.GetName(),
							clusterName,
						)
					}
					return errSimulated
				},
			},
			expectedEvents: []string{"Warning FailedApply Failed to update status"},
			wantErrMsg:     "failed to patch status",
		},
		"Error: getGlobalTopoRef Failed (Cells)": {
			// Note: With caching, we can't rely on counting Get calls.
			// Instead, we omit the template from existingObjects and inject
			// error on the first (and only) Get attempt.
			existingObjects: []client.Object{cellTpl, shardTpl}, // coreTpl intentionally missing
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnKeyName("default-core", errSimulated),
			},
			// Global topo resolution fails early in reconcileGlobalComponents
			expectedEvents: []string{"Warning FailedApply Failed to reconcile global components"},
			wantErrMsg:     "failed to resolve global topo",
		},
		"Success: External Global Topo Resolution": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
					External: &multigresv1alpha1.ExternalTopoServerSpec{
						Endpoints: []multigresv1alpha1.EndpointUrl{"http://external:2379"},
						RootPath:  "/custom/root",
					},
				}
			},
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			expectedEvents:  []string{"Normal Synced Successfully reconciled MultigresCluster"},
			validate: func(t testing.TB, c client.Client) {
				cell := &multigresv1alpha1.Cell{}
				cellName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					"zone-a",
				)
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: cellName, Namespace: namespace},
					cell,
				); err != nil {
					t.Fatalf("Expected Cell %s to exist: %v", cellName, err)
				}
				if cell.Spec.GlobalTopoServer.Address != "http://external:2379" {
					t.Errorf(
						"Expected external address http://external:2379, got %s",
						cell.Spec.GlobalTopoServer.Address,
					)
				}
				if cell.Spec.GlobalTopoServer.RootPath != "/custom/root" {
					t.Errorf(
						"Expected external root path /custom/root, got %s",
						cell.Spec.GlobalTopoServer.RootPath,
					)
				}
			},
		},
		"Success: Early Return on Deletion": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				now := metav1.Now()
				c.DeletionTimestamp = &now
				c.Finalizers = []string{
					"test.finalizer",
				} // Prevent immediate deletion by fake client
			},
			// Expect NO event (returns early)
			expectedEvents: []string{},
			wantErrMsg:     "",
		},
		"Success: Deletion Cleans Up Cells and TableGroups": {
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				&multigresv1alpha1.Cell{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster-zone-a",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
				},
				&multigresv1alpha1.TableGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster-db1-tg1",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
				},
			},
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				now := metav1.Now()
				c.DeletionTimestamp = &now
				c.Finalizers = []string{
					"test.finalizer",
				}
			},
			expectedEvents: []string{},
			wantErrMsg:     "",
		},
		"Success: Deletion Requeues With Remaining Shards": {
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster-shard",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
				},
			},
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				now := metav1.Now()
				c.DeletionTimestamp = &now
				c.Finalizers = []string{
					"multigres.com/finalizer",
					"test.finalizer",
				}
			},
			expectedEvents: []string{},
			wantErrMsg:     "",
		},
		"Error: Deletion List Cells Failed": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				now := metav1.Now()
				c.DeletionTimestamp = &now
				c.Finalizers = []string{
					"multigres.com/finalizer",
					"test.finalizer",
				}
			},
			failureConfig: &testutil.FailureConfig{
				OnList: testutil.FailObjListAfterNCalls(0, errSimulated),
			},
			wantErrMsg: "failed to list cells",
		},
		"Error: Deletion Delete Cell Failed": {
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				&multigresv1alpha1.Cell{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster-zone-a",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
				},
			},
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				now := metav1.Now()
				c.DeletionTimestamp = &now
				c.Finalizers = []string{
					"multigres.com/finalizer",
					"test.finalizer",
				}
			},
			failureConfig: &testutil.FailureConfig{
				OnDelete: testutil.FailOnObjectName("test-cluster-zone-a", errSimulated),
			},
			wantErrMsg: "failed to delete cell",
		},
		"Error: Deletion List TableGroups Failed": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				now := metav1.Now()
				c.DeletionTimestamp = &now
				c.Finalizers = []string{
					"multigres.com/finalizer",
					"test.finalizer",
				}
			},
			failureConfig: &testutil.FailureConfig{
				OnList: testutil.FailObjListAfterNCalls(1, errSimulated),
			},
			wantErrMsg: "failed to list tablegroups",
		},
		"Error: Deletion Delete TableGroup Failed": {
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				&multigresv1alpha1.TableGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster-db1-tg1",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
				},
			},
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				now := metav1.Now()
				c.DeletionTimestamp = &now
				c.Finalizers = []string{
					"multigres.com/finalizer",
					"test.finalizer",
				}
			},
			failureConfig: &testutil.FailureConfig{
				OnDelete: testutil.FailOnObjectName("test-cluster-db1-tg1", errSimulated),
			},
			wantErrMsg: "failed to delete tablegroup",
		},

		"Success: Reconcile With Fresh Trace Context": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Annotations = map[string]string{
					"multigres.com/traceparent":    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
					"multigres.com/traceparent-ts": strconv.FormatInt(time.Now().Unix(), 10),
				}
			},
			expectedEvents: []string{"Normal Synced Successfully reconciled MultigresCluster"},
		},
		"Success: Reconcile With Stale Trace Context": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Annotations = map[string]string{
					"multigres.com/traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
					"multigres.com/traceparent-ts": strconv.FormatInt(
						time.Now().Add(-20*time.Minute).Unix(),
						10,
					),
				}
			},
			expectedEvents: []string{"Normal Synced Successfully reconciled MultigresCluster"},
		},
		"Success: External Implementation Override": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
					External: &multigresv1alpha1.ExternalTopoServerSpec{
						Endpoints:      []multigresv1alpha1.EndpointUrl{"http://external:2379"},
						Implementation: "consul",
					},
				}
			},
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			expectedEvents:  []string{"Normal Synced Successfully reconciled MultigresCluster"},
		},
		"Success: Etcd RootPath Override": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
					TemplateRef: "default-core",
					Etcd: &multigresv1alpha1.EtcdSpec{
						RootPath: "/custom/etcd/root",
					},
				}
			},
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			expectedEvents:  []string{"Normal Synced Successfully reconciled MultigresCluster"},
		},
	}

	runReconcileTest(t, tests)
}

func TestSetupWithManager_Coverage(t *testing.T) {
	t.Parallel()

	// Test the default path (no options)
	t.Run("No Options", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Recovered expected panic: %v", r)
			}
		}()
		reconciler := &MultigresClusterReconciler{}
		_ = reconciler.SetupWithManager(nil)
	})

	// Test the path with options to ensure coverage of the 'if len(opts) > 0' block
	t.Run("With Options", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Recovered expected panic: %v", r)
			}
		}()
		reconciler := &MultigresClusterReconciler{}
		_ = reconciler.SetupWithManager(nil, controller.Options{MaxConcurrentReconciles: 1})
	})
}

func TestEnqueueRequestsFromTemplate(t *testing.T) {
	scheme := setupScheme()

	// Cluster that references "prod-core" as CoreTemplate.
	clusterWithCore := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-core", Namespace: "default"},
		Status: multigresv1alpha1.MultigresClusterStatus{
			ResolvedTemplates: &multigresv1alpha1.ResolvedTemplates{
				CoreTemplates: []multigresv1alpha1.TemplateRef{"prod-core"},
			},
		},
	}
	// Cluster that references "prod-shard" as ShardTemplate.
	clusterWithShard := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-shard", Namespace: "default"},
		Status: multigresv1alpha1.MultigresClusterStatus{
			ResolvedTemplates: &multigresv1alpha1.ResolvedTemplates{
				ShardTemplates: []multigresv1alpha1.TemplateRef{"prod-shard"},
			},
		},
	}
	// Cluster with nil resolvedTemplates (never reconciled).
	clusterNilStatus := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-nil", Namespace: "default"},
	}
	// Cluster in different namespace.
	clusterOtherNS := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-other", Namespace: "other"},
		Status: multigresv1alpha1.MultigresClusterStatus{
			ResolvedTemplates: &multigresv1alpha1.ResolvedTemplates{
				CoreTemplates: []multigresv1alpha1.TemplateRef{"prod-core"},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(clusterWithCore, clusterWithShard, clusterNilStatus, clusterOtherNS).
		Build()

	r := &MultigresClusterReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	t.Run("CoreTemplate matches only referencing and nil-status clusters", func(t *testing.T) {
		tpl := &multigresv1alpha1.CoreTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "prod-core", Namespace: "default"},
		}
		requests := r.enqueueRequestsFromTemplate(context.Background(), tpl)

		names := make(map[string]bool)
		for _, req := range requests {
			names[req.Name] = true
		}
		if !names["cluster-core"] {
			t.Error("Expected cluster-core (references prod-core) to be enqueued")
		}
		if !names["cluster-nil"] {
			t.Error("Expected cluster-nil (nil status) to be enqueued")
		}
		if names["cluster-shard"] {
			t.Error("cluster-shard should not be enqueued for CoreTemplate change")
		}
		if names["cluster-other"] {
			t.Error("cluster-other (different namespace) should not be enqueued")
		}
		if len(requests) != 2 {
			t.Errorf("Expected 2 requests, got %d: %v", len(requests), names)
		}
	})

	t.Run("ShardTemplate matches only referencing and nil-status clusters", func(t *testing.T) {
		tpl := &multigresv1alpha1.ShardTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "prod-shard", Namespace: "default"},
		}
		requests := r.enqueueRequestsFromTemplate(context.Background(), tpl)

		names := make(map[string]bool)
		for _, req := range requests {
			names[req.Name] = true
		}
		if !names["cluster-shard"] {
			t.Error("Expected cluster-shard to be enqueued")
		}
		if !names["cluster-nil"] {
			t.Error("Expected cluster-nil (nil status) to be enqueued")
		}
		if names["cluster-core"] {
			t.Error("cluster-core should not be enqueued for ShardTemplate change")
		}
		if len(requests) != 2 {
			t.Errorf("Expected 2 requests, got %d: %v", len(requests), names)
		}
	})

	t.Run("Unmatched template enqueues only nil-status clusters", func(t *testing.T) {
		tpl := &multigresv1alpha1.CellTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "nonexistent-cell", Namespace: "default"},
		}
		requests := r.enqueueRequestsFromTemplate(context.Background(), tpl)

		if len(requests) != 1 {
			t.Errorf("Expected 1 request (nil-status cluster only), got %d", len(requests))
		}
		if len(requests) == 1 && requests[0].Name != "cluster-nil" {
			t.Errorf("Expected cluster-nil, got %s", requests[0].Name)
		}
	})

	t.Run("Unknown object type returns nil", func(t *testing.T) {
		unknown := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "not-a-template", Namespace: "default"},
		}
		requests := r.enqueueRequestsFromTemplate(context.Background(), unknown)
		if requests != nil {
			t.Errorf("Expected nil for unknown object type, got %v", requests)
		}
	})

	t.Run("List error returns empty", func(t *testing.T) {
		failureConfig := &testutil.FailureConfig{
			OnList: testutil.FailObjListAfterNCalls(0, errors.New("list error")),
		}
		r.Client = testutil.NewFakeClientWithFailures(fakeClient, failureConfig)
		defer func() { r.Client = fakeClient }()

		tpl := &multigresv1alpha1.CoreTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "prod-core", Namespace: "default"},
		}
		requests := r.enqueueRequestsFromTemplate(context.Background(), tpl)
		if len(requests) != 0 {
			t.Errorf("Expected 0 requests on list error, got %d", len(requests))
		}
	})
}

func TestTemplateKindFromObject(t *testing.T) {
	tests := []struct {
		name string
		obj  client.Object
		want string
	}{
		{"CoreTemplate", &multigresv1alpha1.CoreTemplate{}, "CoreTemplate"},
		{"CellTemplate", &multigresv1alpha1.CellTemplate{}, "CellTemplate"},
		{"ShardTemplate", &multigresv1alpha1.ShardTemplate{}, "ShardTemplate"},
		{"Unknown type", &multigresv1alpha1.MultigresCluster{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := templateKindFromObject(tt.obj); got != tt.want {
				t.Errorf("templateKindFromObject() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReferencesTemplate(t *testing.T) {
	rt := &multigresv1alpha1.ResolvedTemplates{
		CoreTemplates:  []multigresv1alpha1.TemplateRef{"core-a"},
		CellTemplates:  []multigresv1alpha1.TemplateRef{"cell-a", "cell-b"},
		ShardTemplates: []multigresv1alpha1.TemplateRef{"shard-a"},
	}

	tests := []struct {
		name string
		rt   *multigresv1alpha1.ResolvedTemplates
		kind string
		tpl  string
		want bool
	}{
		{"nil status always matches", nil, "CoreTemplate", "anything", true},
		{"CoreTemplate match", rt, "CoreTemplate", "core-a", true},
		{"CoreTemplate no match", rt, "CoreTemplate", "core-x", false},
		{"CellTemplate match first", rt, "CellTemplate", "cell-a", true},
		{"CellTemplate match second", rt, "CellTemplate", "cell-b", true},
		{"CellTemplate no match", rt, "CellTemplate", "cell-x", false},
		{"ShardTemplate match", rt, "ShardTemplate", "shard-a", true},
		{"ShardTemplate no match", rt, "ShardTemplate", "shard-x", false},
		{"Unknown kind", rt, "UnknownKind", "anything", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := referencesTemplate(tt.rt, tt.kind, tt.tpl); got != tt.want {
				t.Errorf("referencesTemplate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectResolvedTemplates(t *testing.T) {
	t.Run("All template refs populated", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TemplateDefaults: multigresv1alpha1.TemplateDefaults{
					CoreTemplate:  "default-core",
					CellTemplate:  "default-cell",
					ShardTemplate: "default-shard",
				},
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					TemplateRef: "gts-core",
				},
				Multiadmin: &multigresv1alpha1.MultiadminConfig{
					TemplateRef: "admin-core",
				},
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "z1", CellTemplate: "cell-ha"},
					{Name: "z2", CellTemplate: "cell-std"},
				},
				Databases: []multigresv1alpha1.DatabaseConfig{
					{
						TableGroups: []multigresv1alpha1.TableGroupConfig{
							{
								Shards: []multigresv1alpha1.ShardConfig{
									{ShardTemplate: "shard-prod"},
								},
							},
						},
					},
				},
			},
		}

		rt := collectResolvedTemplates(cluster)

		wantCore := []multigresv1alpha1.TemplateRef{"admin-core", "default-core", "gts-core"}
		if !slices.Equal(rt.CoreTemplates, wantCore) {
			t.Errorf("CoreTemplates = %v, want %v", rt.CoreTemplates, wantCore)
		}
		wantCell := []multigresv1alpha1.TemplateRef{"cell-ha", "cell-std", "default-cell"}
		if !slices.Equal(rt.CellTemplates, wantCell) {
			t.Errorf("CellTemplates = %v, want %v", rt.CellTemplates, wantCell)
		}
		wantShard := []multigresv1alpha1.TemplateRef{"default-shard", "shard-prod"}
		if !slices.Equal(rt.ShardTemplates, wantShard) {
			t.Errorf("ShardTemplates = %v, want %v", rt.ShardTemplates, wantShard)
		}
	})

	t.Run("Duplicates are deduplicated", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TemplateDefaults: multigresv1alpha1.TemplateDefaults{
					CoreTemplate:  "same-core",
					CellTemplate:  "same-cell",
					ShardTemplate: "same-shard",
				},
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					TemplateRef: "same-core",
				},
				Multiadmin: &multigresv1alpha1.MultiadminConfig{
					TemplateRef: "same-core",
				},
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "z1", CellTemplate: "same-cell"},
					{Name: "z2", CellTemplate: "same-cell"},
				},
				Databases: []multigresv1alpha1.DatabaseConfig{
					{
						TableGroups: []multigresv1alpha1.TableGroupConfig{
							{
								Shards: []multigresv1alpha1.ShardConfig{
									{ShardTemplate: "same-shard"},
								},
							},
						},
					},
				},
			},
		}

		rt := collectResolvedTemplates(cluster)

		if len(rt.CoreTemplates) != 1 || rt.CoreTemplates[0] != "same-core" {
			t.Errorf("CoreTemplates = %v, want [same-core]", rt.CoreTemplates)
		}
		if len(rt.CellTemplates) != 1 || rt.CellTemplates[0] != "same-cell" {
			t.Errorf("CellTemplates = %v, want [same-cell]", rt.CellTemplates)
		}
		if len(rt.ShardTemplates) != 1 || rt.ShardTemplates[0] != "same-shard" {
			t.Errorf("ShardTemplates = %v, want [same-shard]", rt.ShardTemplates)
		}
	})

	t.Run("No templates (pure inline)", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "z1", Spec: &multigresv1alpha1.CellInlineSpec{}},
				},
			},
		}

		rt := collectResolvedTemplates(cluster)

		if len(rt.CoreTemplates) != 0 {
			t.Errorf("CoreTemplates should be empty, got %v", rt.CoreTemplates)
		}
		if len(rt.CellTemplates) != 0 {
			t.Errorf("CellTemplates should be empty, got %v", rt.CellTemplates)
		}
		if len(rt.ShardTemplates) != 0 {
			t.Errorf("ShardTemplates should be empty, got %v", rt.ShardTemplates)
		}
	})

	t.Run("MultiadminWeb templateRef included", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				MultiadminWeb: &multigresv1alpha1.MultiadminWebConfig{
					TemplateRef: "web-core",
				},
			},
		}

		rt := collectResolvedTemplates(cluster)

		if len(rt.CoreTemplates) != 1 || rt.CoreTemplates[0] != "web-core" {
			t.Errorf("CoreTemplates = %v, want [web-core]", rt.CoreTemplates)
		}
	})
}

func TestCollectTrackingLabels(t *testing.T) {
	t.Run("All template kinds referenced", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TemplateDefaults: multigresv1alpha1.TemplateDefaults{
					CoreTemplate:  "default-core",
					CellTemplate:  "default-cell",
					ShardTemplate: "default-shard",
				},
			},
		}

		labels := collectTrackingLabels(cluster)

		if labels[metadata.LabelUsesCoreTemplate] != "true" {
			t.Error("Expected uses-core-template=true")
		}
		if labels[metadata.LabelUsesCellTemplate] != "true" {
			t.Error("Expected uses-cell-template=true")
		}
		if labels[metadata.LabelUsesShardTemplate] != "true" {
			t.Error("Expected uses-shard-template=true")
		}
	})

	t.Run("No templates (pure inline)", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "z1", Spec: &multigresv1alpha1.CellInlineSpec{}},
				},
			},
		}

		labels := collectTrackingLabels(cluster)

		if len(labels) != 0 {
			t.Errorf("Expected no tracking labels for inline-only cluster, got %v", labels)
		}
	})

	t.Run("Only cell template from per-cell ref", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "z1", CellTemplate: "cell-ha"},
				},
			},
		}

		labels := collectTrackingLabels(cluster)

		if _, ok := labels[metadata.LabelUsesCoreTemplate]; ok {
			t.Error("Unexpected uses-core-template label")
		}
		if labels[metadata.LabelUsesCellTemplate] != "true" {
			t.Error("Expected uses-cell-template=true")
		}
		if _, ok := labels[metadata.LabelUsesShardTemplate]; ok {
			t.Error("Unexpected uses-shard-template label")
		}
	})

	t.Run("Only shard template from per-shard ref", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Databases: []multigresv1alpha1.DatabaseConfig{
					{
						TableGroups: []multigresv1alpha1.TableGroupConfig{
							{
								Shards: []multigresv1alpha1.ShardConfig{
									{ShardTemplate: "shard-prod"},
								},
							},
						},
					},
				},
			},
		}

		labels := collectTrackingLabels(cluster)

		if _, ok := labels[metadata.LabelUsesCoreTemplate]; ok {
			t.Error("Unexpected uses-core-template label")
		}
		if _, ok := labels[metadata.LabelUsesCellTemplate]; ok {
			t.Error("Unexpected uses-cell-template label")
		}
		if labels[metadata.LabelUsesShardTemplate] != "true" {
			t.Error("Expected uses-shard-template=true")
		}
	})

	t.Run("Core from GlobalTopoServer templateRef", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					TemplateRef: "gts-core",
				},
			},
		}

		labels := collectTrackingLabels(cluster)

		if labels[metadata.LabelUsesCoreTemplate] != "true" {
			t.Error("Expected uses-core-template=true from GlobalTopoServer ref")
		}
	})

	t.Run("Core from MultiadminWeb templateRef", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				MultiadminWeb: &multigresv1alpha1.MultiadminWebConfig{
					TemplateRef: "web-core",
				},
			},
		}

		labels := collectTrackingLabels(cluster)

		if labels[metadata.LabelUsesCoreTemplate] != "true" {
			t.Error("Expected uses-core-template=true from MultiadminWeb ref")
		}
	})
}

func TestReconciler_PatchTrackingLabelsError(t *testing.T) {
	scheme := setupScheme()
	coreTpl, cellTpl, shardTpl, baseCluster, clusterName, namespace := setupFixtures(t)
	baseCluster.Labels = nil // trigger patch

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(coreTpl, cellTpl, shardTpl, baseCluster).
		Build()

	failureConfig := &testutil.FailureConfig{
		OnPatch: testutil.FailOnObjectName(clusterName, errors.New("simulated patch error")),
	}
	r := &MultigresClusterReconciler{
		Client:   testutil.NewFakeClientWithFailures(baseClient, failureConfig),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	_, err := r.Reconcile(
		context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace}},
	)
	if err == nil || !strings.Contains(err.Error(), "failed to patch tracking labels") {
		t.Errorf("Expected patch error, got %v", err)
	}
}

func TestReconciler_TopologyFailure(t *testing.T) {
	scheme := setupScheme()
	coreTpl, cellTpl, shardTpl, baseCluster, clusterName, namespace := setupFixtures(t)

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(coreTpl, cellTpl, shardTpl, baseCluster).
		WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
		Build()

	r := &MultigresClusterReconciler{
		Client:   baseClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return nil, errors.New("simulated topo error")
		},
	}
	_, err := r.Reconcile(
		context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace}},
	)
	if err == nil || !strings.Contains(err.Error(), "failed to open topology store") {
		t.Errorf("Expected topology reconcile error, got %v", err)
	}
}

func TestReconciler_TopologyRequeueWhenUnavailable(t *testing.T) {
	scheme := setupScheme()
	coreTpl, cellTpl, shardTpl, baseCluster, clusterName, namespace := setupFixtures(t)
	baseCluster.CreationTimestamp = metav1.Now() // Trigger grace period

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(coreTpl, cellTpl, shardTpl, baseCluster).
		WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
		Build()

	r := &MultigresClusterReconciler{
		Client:   baseClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return nil, errors.New("UNAVAILABLE: topo not ready")
		},
	}
	res, err := r.Reconcile(
		context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace}},
	)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter > 0")
	}
}

func TestReconciler_TopologyErrorWhenUnavailableExpired(t *testing.T) {
	scheme := setupScheme()
	coreTpl, cellTpl, shardTpl, baseCluster, clusterName, namespace := setupFixtures(t)

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(coreTpl, cellTpl, shardTpl, baseCluster).
		WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
		Build()

	r := &MultigresClusterReconciler{
		Client:   baseClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return nil, errors.New("UNAVAILABLE: topo not ready")
		},
	}
	_, err := r.Reconcile(
		context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace}},
	)
	if err == nil || !strings.Contains(err.Error(), "topology server unavailable") {
		t.Errorf("Expected topology unavailable error, got %v", err)
	}
}
