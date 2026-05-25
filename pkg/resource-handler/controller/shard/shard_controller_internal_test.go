package shard

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	ctrl "sigs.k8s.io/controller-runtime"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// Helper functions moved from shard_controller_test_util_test.go

func buildHashedPoolName(shard *multigresv1alpha1.Shard, poolName, cellName string) string {
	clusterName := shard.Labels["multigres.com/cluster"]
	return name.JoinWithConstraints(
		name.StatefulSetConstraints,
		clusterName,
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
		"pool",
		poolName,
		cellName,
	)
}

func buildHashedPoolHeadlessServiceName(
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
) string {
	clusterName := shard.Labels["multigres.com/cluster"]
	return name.JoinWithConstraints(
		name.ServiceConstraints,
		clusterName,
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
		"pool",
		poolName,
		cellName,
		"headless",
	)
}

func buildHashedBackupPVCName(shard *multigresv1alpha1.Shard, cellName string) string {
	clusterName := shard.Labels["multigres.com/cluster"]
	return name.JoinWithConstraints(
		name.ServiceConstraints,
		"backup-data",
		clusterName,
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
		cellName,
	)
}

func buildHashedMultiOrchName(shard *multigresv1alpha1.Shard, cellName string) string {
	clusterName := shard.Labels["multigres.com/cluster"]
	return name.JoinWithConstraints(
		name.ServiceConstraints,
		clusterName,
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
		"multiorch",
		cellName,
	)
}

func TestGetPoolCells(t *testing.T) {
	tests := map[string]struct {
		shard *multigresv1alpha1.Shard
		want  []multigresv1alpha1.CellName
	}{
		"explicit multiorch and pools in different cells": {
			shard: &multigresv1alpha1.Shard{
				Spec: multigresv1alpha1.ShardSpec{
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						Cells: []multigresv1alpha1.CellName{"zone-a"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"pool1": {Cells: []multigresv1alpha1.CellName{"zone-b"}},
					},
				},
			},
			want: []multigresv1alpha1.CellName{"zone-b"},
		},
		"only multiorch": {
			shard: &multigresv1alpha1.Shard{
				Spec: multigresv1alpha1.ShardSpec{
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						Cells: []multigresv1alpha1.CellName{"zone-a"},
					},
				},
			},
			want: []multigresv1alpha1.CellName{},
		},
		"only pools": {
			shard: &multigresv1alpha1.Shard{
				Spec: multigresv1alpha1.ShardSpec{
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"pool1": {Cells: []multigresv1alpha1.CellName{"zone-b"}},
						"pool2": {Cells: []multigresv1alpha1.CellName{"zone-c"}},
					},
				},
			},
			want: []multigresv1alpha1.CellName{"zone-b", "zone-c"},
		},
		"overlapping cells": {
			shard: &multigresv1alpha1.Shard{
				Spec: multigresv1alpha1.ShardSpec{
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						Cells: []multigresv1alpha1.CellName{"zone-a", "zone-b"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"pool1": {Cells: []multigresv1alpha1.CellName{"zone-b", "zone-c"}},
					},
				},
			},
			want: []multigresv1alpha1.CellName{"zone-b", "zone-c"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := getPoolCells(tc.shard)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("getPoolCells() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSetConditions(t *testing.T) {
	tests := map[string]struct {
		generation int64
		totalPods  int32
		readyPods  int32
		want       []metav1.Condition
	}{
		"all pods ready": {
			generation: 5,
			totalPods:  3,
			readyPods:  3,
			want: []metav1.Condition{
				{
					Type:               "Available",
					Status:             metav1.ConditionTrue,
					Reason:             "AllPodsReady",
					Message:            "All 3 pods are ready",
					ObservedGeneration: 5,
				},
			},
		},
		"partial pods ready": {
			generation: 10,
			totalPods:  5,
			readyPods:  2,
			want: []metav1.Condition{
				{
					Type:               "Available",
					Status:             metav1.ConditionFalse,
					Reason:             "NotAllPodsReady",
					Message:            "2/5 pods ready",
					ObservedGeneration: 10,
				},
			},
		},
		"no pods": {
			generation: 1,
			totalPods:  0,
			readyPods:  0,
			want: []metav1.Condition{
				{
					Type:               "Available",
					Status:             metav1.ConditionFalse,
					Reason:             "NotAllPodsReady",
					Message:            "0/0 pods ready",
					ObservedGeneration: 1,
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			shard := &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Generation: tc.generation},
			}
			r := &ShardReconciler{
				Recorder: record.NewFakeRecorder(100),
			}
			r.setConditions(shard, tc.totalPods, tc.readyPods)
			got := shard.Status.Conditions

			// Use go-cmp for exact match, ignoring LastTransitionTime
			opts := cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime")
			if diff := cmp.Diff(tc.want, got, opts); diff != "" {
				t.Errorf("setConditions() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestBuildMultiOrchContainer_WithImage tests buildMultiOrchContainer with custom image.
// This tests the image override path that was missing coverage.
func TestBuildMultiOrchContainer_WithImage(t *testing.T) {
	customImage := "custom/multiorch:v1.2.3"
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.ShardSpec{
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			Images: multigresv1alpha1.ShardImages{
				MultiOrch: multigresv1alpha1.ImageRef(customImage),
			},
		},
	}

	container := buildMultiOrchContainer(shard, "zone1")

	if container.Image != customImage {
		t.Errorf("buildMultiOrchContainer() image = %s, want %s", container.Image, customImage)
	}
	if container.Name != "multiorch" {
		t.Errorf("buildMultiOrchContainer() name = %s, want multiorch", container.Name)
	}
}

// TestReconcile_InvalidScheme tests the error path when Build* functions fail due to invalid scheme.
// This should never happen in production - scheme is properly set up in main.go.
// Test exists for coverage of defensive error handling.
func TestReconcile_InvalidScheme(t *testing.T) {
	tests := map[string]struct {
		setupShard    func() *multigresv1alpha1.Shard
		reconcileFunc func(*ShardReconciler, context.Context, *multigresv1alpha1.Shard) error
	}{
		"MultiOrchDeployment": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
					Spec: multigresv1alpha1.ShardSpec{
						MultiOrch: multigresv1alpha1.MultiOrchSpec{
							Cells: []multigresv1alpha1.CellName{"cell1"},
						},
					},
				}
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				return r.reconcileMultiOrchDeployment(ctx, shard, "cell1")
			},
		},
		"MultiOrchService": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
				}
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				return r.reconcileMultiOrchService(ctx, shard, "cell1")
			},
		},
		"PoolPDB": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
				}
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				return r.reconcilePoolPDB(ctx, shard, "pool1", "cell1")
			},
		},
		"PoolHeadlessService": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
				}
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				poolSpec := multigresv1alpha1.PoolSpec{
					Cells: []multigresv1alpha1.CellName{"cell1"},
				}
				return r.reconcilePoolHeadlessService(ctx, shard, "pool1", "", poolSpec)
			},
		},
		"SharedBackupPVC": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
					Spec: multigresv1alpha1.ShardSpec{
						Backup: &multigresv1alpha1.BackupConfig{
							Type:       multigresv1alpha1.BackupTypeFilesystem,
							Filesystem: &multigresv1alpha1.FilesystemBackupConfig{},
						},
					},
				}
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				return r.reconcileSharedBackupPVC(ctx, shard, "cell1")
			},
		},
		"PostgresPasswordSecret": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
				}
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				return r.reconcilePostgresPasswordSecret(ctx, shard)
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Empty scheme without Shard type registered
			invalidScheme := runtime.NewScheme()

			shard := tc.setupShard()

			fakeClient := fake.NewClientBuilder().
				WithScheme(invalidScheme).
				Build()

			reconciler := &ShardReconciler{
				Client:    fakeClient,
				Scheme:    invalidScheme,
				Recorder:  record.NewFakeRecorder(100),
				APIReader: fakeClient,
			}

			err := tc.reconcileFunc(reconciler, context.Background(), shard)
			if err == nil {
				t.Errorf("reconcile function should error with invalid scheme")
			}
		})
	}
}

// TestUpdateStatus_PoolPodsNotFound tests the NotFound path in updateStatus.
func TestUpdateStatus_PoolPodsNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.ShardSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {
					Cells: []multigresv1alpha1.CellName{"cell1"},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	reconciler := &ShardReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		Recorder:  record.NewFakeRecorder(100),
		APIReader: fakeClient,
	}

	// Call updateStatus when pool Pods don't exist yet
	err := reconciler.updateStatus(context.Background(), shard)
	if err != nil {
		t.Errorf("updateStatus() should not error when pool Pods not found, got: %v", err)
	}
}

// TestReconcile_PatchError tests error path on Patch operations.
func TestReconcile_PatchError(t *testing.T) {
	tests := map[string]struct {
		setupShard    func() *multigresv1alpha1.Shard
		getFailObj    func(*multigresv1alpha1.Shard) string
		reconcileFunc func(*ShardReconciler, context.Context, *multigresv1alpha1.Shard) error
	}{
		"MultiOrchDeployment": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
					Spec: multigresv1alpha1.ShardSpec{
						DatabaseName:   "testdb",
						TableGroupName: "default",
						MultiOrch: multigresv1alpha1.MultiOrchSpec{
							Cells: []multigresv1alpha1.CellName{"cell1"},
						},
					},
				}
			},
			getFailObj: func(s *multigresv1alpha1.Shard) string {
				return buildHashedMultiOrchName(s, "cell1")
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				return r.reconcileMultiOrchDeployment(ctx, shard, "cell1")
			},
		},
		"MultiOrchService": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
					Spec: multigresv1alpha1.ShardSpec{
						DatabaseName:   "testdb",
						TableGroupName: "default",
					},
				}
			},
			getFailObj: func(s *multigresv1alpha1.Shard) string {
				return buildHashedMultiOrchName(s, "cell1")
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				return r.reconcileMultiOrchService(ctx, shard, "cell1")
			},
		},
		"PoolPDB": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
					Spec: multigresv1alpha1.ShardSpec{
						DatabaseName:   "testdb",
						TableGroupName: "default",
					},
				}
			},
			getFailObj: func(s *multigresv1alpha1.Shard) string {
				// The PDB name formula is from BuildPoolPodDisruptionBudget
				clusterName := s.Labels["multigres.com/cluster"]
				return name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					string(s.Spec.DatabaseName),
					string(s.Spec.TableGroupName),
					string(s.Spec.ShardName),
					"pool",
					"pool1",
					"cell1",
					"pdb",
				)
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				return r.reconcilePoolPDB(ctx, shard, "pool1", "cell1")
			},
		},
		"PoolHeadlessService": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
					Spec: multigresv1alpha1.ShardSpec{
						DatabaseName:   "testdb",
						TableGroupName: "default",
					},
				}
			},
			getFailObj: func(s *multigresv1alpha1.Shard) string {
				return buildHashedPoolHeadlessServiceName(s, "pool1", "cell1")
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				poolSpec := multigresv1alpha1.PoolSpec{
					Cells: []multigresv1alpha1.CellName{"cell1"},
				}
				return r.reconcilePoolHeadlessService(ctx, shard, "pool1", "cell1", poolSpec)
			},
		},
		"PgHbaConfigMap": {
			setupShard: func() *multigresv1alpha1.Shard {
				return &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
					Spec: multigresv1alpha1.ShardSpec{
						DatabaseName:   "testdb",
						TableGroupName: "default",
					},
				}
			},
			getFailObj: func(s *multigresv1alpha1.Shard) string {
				return PgHbaConfigMapName(s.Name)
			},
			reconcileFunc: func(r *ShardReconciler, ctx context.Context, shard *multigresv1alpha1.Shard) error {
				return r.reconcilePgHbaConfigMap(ctx, shard)
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = multigresv1alpha1.AddToScheme(scheme)
			_ = appsv1.AddToScheme(scheme)
			_ = corev1.AddToScheme(scheme)
			_ = policyv1.AddToScheme(scheme)

			shard := tc.setupShard()
			failObj := tc.getFailObj(shard)

			// Create client with failure injection
			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(shard).
				Build()

			fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if obj.GetName() == failObj {
						return testutil.ErrNetworkTimeout
					}
					return nil
				},
			})

			reconciler := &ShardReconciler{
				Client:    fakeClient,
				Scheme:    scheme,
				Recorder:  record.NewFakeRecorder(100),
				APIReader: fakeClient,
			}

			err := tc.reconcileFunc(reconciler, context.Background(), shard)
			if err == nil {
				t.Errorf("reconcile function should error on Patch failure")
			}
		})
	}
}

// TestReconcile_PostgresSecretError verifies the error path in Reconcile when
// reconcilePostgresPasswordSecret fails (lines 81-92 of shard_controller.go).
func TestReconcile_PostgresSecretError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: "missing-postgres-password",
				Key:  PostgresPasswordSecretKey,
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {
					ReplicasPerCell: ptr.To(int32(1)),
					Cells:           []multigresv1alpha1.CellName{"cell1"},
				},
			},
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	reconciler := &ShardReconciler{
		Client:          baseClient,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(100),
		APIReader:       baseClient,
		CreateTopoStore: newMemoryTopoFactory(),
	}

	req := ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(shard),
	}

	_, err := reconciler.Reconcile(t.Context(), req)
	if err == nil {
		t.Fatal("Reconcile should return an error when reconcilePostgresPasswordSecret fails")
	}
	if !strings.Contains(
		err.Error(),
		`failed to get postgres password Secret "missing-postgres-password"`,
	) {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestUpdateStatus_MultiOrch tests updateStatus with different MultiOrch deployment scenarios.
func TestUpdateStatus_MultiOrch(t *testing.T) {
	tests := map[string]struct {
		setupObjects    []client.Object
		expectError     bool
		expectOrchReady bool
		setupClient     func(*testing.T, *runtime.Scheme, *multigresv1alpha1.Shard) client.Client
		customShard     *multigresv1alpha1.Shard // Optional: override default shard
	}{
		"GetError": {
			expectError: true,
			setupClient: func(t *testing.T, scheme *runtime.Scheme, shard *multigresv1alpha1.Shard) client.Client {
				baseClient := fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(shard).
					WithStatusSubresource(&multigresv1alpha1.Shard{}).
					Build()
				return testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
					OnGet: testutil.FailOnKeyName(
						buildHashedMultiOrchName(shard, "zone1"),
						testutil.ErrNetworkTimeout,
					),
				})
			},
		},
		"NotReady": {
			expectError:     false,
			expectOrchReady: false,
			setupObjects: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-multiorch-zone1",
						Namespace: "default",
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(3)),
					},
					Status: appsv1.DeploymentStatus{
						ReadyReplicas: 1, // Not all ready
					},
				},
			},
		},
		"NilReplicas": {
			expectError:     false,
			expectOrchReady: false,
			setupObjects: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-multiorch-zone1",
						Namespace: "default",
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: nil, // Nil replicas
					},
				},
			},
		},
		"NotFound": {
			expectError:     false,
			expectOrchReady: false,
			setupObjects:    []client.Object{}, // No MultiOrch Deployment - will get NotFound
		},
		"NoCellsInMultiOrchOrPools": {
			expectError:     false,
			expectOrchReady: false,
			setupObjects:    []client.Object{},
			customShard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						Cells: []multigresv1alpha1.CellName{}, // Empty
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{}, // Empty
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = multigresv1alpha1.AddToScheme(scheme)
			_ = appsv1.AddToScheme(scheme)

			// Use custom shard if provided, otherwise use default
			shard := tc.customShard
			if shard == nil {
				shard = &multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard",
						Namespace: "default",
					},
					Spec: multigresv1alpha1.ShardSpec{
						MultiOrch: multigresv1alpha1.MultiOrchSpec{
							Cells: []multigresv1alpha1.CellName{"zone1"},
						},
						Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
					},
				}
			}

			var fakeClient client.Client
			if tc.setupClient != nil {
				fakeClient = tc.setupClient(t, scheme, shard)
			} else {
				objects := append([]client.Object{shard}, tc.setupObjects...)
				fakeClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(objects...).
					WithStatusSubresource(&multigresv1alpha1.Shard{}).
					Build()
			}

			reconciler := &ShardReconciler{
				Client:    fakeClient,
				Scheme:    scheme,
				Recorder:  record.NewFakeRecorder(100),
				APIReader: fakeClient,
			}

			err := reconciler.updateStatus(context.Background(), shard)
			if tc.expectError && err == nil {
				t.Error("updateStatus() should error but didn't")
			}
			if !tc.expectError && err != nil {
				t.Errorf("updateStatus() unexpected error: %v", err)
			}

			// For non-error cases, verify OrchReady status
			if !tc.expectError {
				updatedShard := &multigresv1alpha1.Shard{}
				if err := fakeClient.Get(
					context.Background(),
					client.ObjectKeyFromObject(shard),
					updatedShard,
				); err != nil {
					t.Fatalf("Failed to get shard: %v", err)
				}

				if updatedShard.Status.OrchReady != tc.expectOrchReady {
					t.Errorf(
						"OrchReady = %v, want %v",
						updatedShard.Status.OrchReady,
						tc.expectOrchReady,
					)
				}
			}
		})
	}
}

// TestUpdateStatus_ListError tests error path on List pool Pods.
func TestUpdateStatus_GetError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.ShardSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {
					Cells: []multigresv1alpha1.CellName{"cell1"},
				},
			},
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
		OnList: func(list client.ObjectList) error {
			if _, ok := list.(*corev1.PodList); ok {
				return testutil.ErrNetworkTimeout
			}
			return nil
		},
	})

	reconciler := &ShardReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		Recorder:  record.NewFakeRecorder(100),
		APIReader: fakeClient,
	}

	err := reconciler.updateStatus(context.Background(), shard)
	if err == nil {
		t.Error("updateStatus() should error on Get failure")
	}
}

// statusPatchCapture wraps a client.Client to snapshot the state of the
// patch options before delegating to the real Status().Patch().
type statusPatchCapture struct {
	client.Client
	capturedOpts []client.SubResourcePatchOption
}

func (c *statusPatchCapture) Status() client.StatusWriter {
	return &capturingStatusWriter{
		StatusWriter: c.Client.Status(),
		capture:      c,
	}
}

type capturingStatusWriter struct {
	client.StatusWriter
	capture *statusPatchCapture
}

func (w *capturingStatusWriter) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.SubResourcePatchOption,
) error {
	w.capture.capturedOpts = opts
	return w.StatusWriter.Patch(ctx, obj, patch, opts...)
}

// TestUpdateStatus_FieldOwner verifies that the SSA status patch uses
// "multigres-resource-handler" as the field owner, not "multigres-operator".
func TestUpdateStatus_FieldOwner(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.ShardSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {
					Cells: []multigresv1alpha1.CellName{"cell1"},
				},
			},
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	capture := &statusPatchCapture{Client: baseClient}

	reconciler := &ShardReconciler{
		Client:    capture,
		Scheme:    scheme,
		Recorder:  record.NewFakeRecorder(100),
		APIReader: baseClient,
	}

	err := reconciler.updateStatus(context.Background(), shard)
	if err != nil {
		t.Fatalf("updateStatus() unexpected error: %v", err)
	}

	// Verify the field owner is "multigres-resource-handler"
	foundFieldOwner := false
	for _, opt := range capture.capturedOpts {
		if fo, ok := opt.(client.FieldOwner); ok {
			if string(fo) != "multigres-resource-handler" {
				t.Errorf("field owner = %q, want %q", string(fo), "multigres-resource-handler")
			}
			foundFieldOwner = true
		}
	}
	if !foundFieldOwner {
		t.Error("no FieldOwner option found in Status().Patch() call")
	}
}

// TestHandleScaleDown_ConcurrentDrainPrevention verifies that handleScaleDown
// respects the inProgress flag: when any pod already has a drain annotation
// (DrainStateRequested, DrainStateDraining, or DrainStateAcknowledged), no new
// drains are initiated for either DRAINED replacement or extra-pod scale-down.
func TestHandleScaleDown_ConcurrentDrainPrevention(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	poolName := "main"
	cellName := "z1"

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
		},
	}

	podName0 := BuildPoolPodName(baseShard, poolName, cellName, 0)
	podName1 := BuildPoolPodName(baseShard, poolName, cellName, 1)
	podName2 := BuildPoolPodName(baseShard, poolName, cellName, 2)

	makePod := func(podName string, annotations map[string]string) *corev1.Pod {
		if annotations == nil {
			annotations = map[string]string{}
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        podName,
				Namespace:   "default",
				Annotations: annotations,
				Labels: map[string]string{
					metadata.LabelMultigresCell: cellName,
				},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
	}

	tests := map[string]struct {
		replicas       int32
		pods           []*corev1.Pod
		podRoles       map[string]string
		actionTaken    bool
		wantAction     bool
		wantInProgress bool
		wantNoDrains   bool
		wantDrainedPod string
	}{
		"drain in progress (DrainStateRequested) blocks DRAINED replacement": {
			replicas: 2,
			pods: []*corev1.Pod{
				makePod(podName0, map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateRequested,
				}),
				makePod(podName1, nil),
			},
			podRoles: map[string]string{
				podName1: "DRAINED",
			},
			wantAction:     false,
			wantInProgress: true,
			wantNoDrains:   true,
		},
		"drain in progress (DrainStateDraining) blocks DRAINED replacement": {
			replicas: 2,
			pods: []*corev1.Pod{
				makePod(podName0, map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateDraining,
				}),
				makePod(podName1, nil),
			},
			podRoles: map[string]string{
				podName1: "DRAINED",
			},
			wantAction:     false,
			wantInProgress: true,
			wantNoDrains:   true,
		},
		"drain in progress (DrainStateAcknowledged) blocks DRAINED replacement": {
			replicas: 2,
			pods: []*corev1.Pod{
				makePod(podName0, map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateAcknowledged,
				}),
				makePod(podName1, nil),
			},
			podRoles: map[string]string{
				podName1: "DRAINED",
			},
			wantAction:     false,
			wantInProgress: true,
			wantNoDrains:   true,
		},
		"drain in progress (DrainStateRequested) blocks extra pod drain": {
			replicas: 1,
			pods: []*corev1.Pod{
				makePod(podName0, map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateRequested,
				}),
				makePod(podName1, nil),
			},
			wantAction:     false,
			wantInProgress: true,
			wantNoDrains:   true,
		},
		"drain in progress (DrainStateDraining) blocks extra pod drain": {
			replicas: 1,
			pods: []*corev1.Pod{
				makePod(podName0, map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateDraining,
				}),
				makePod(podName1, nil),
			},
			wantAction:     false,
			wantInProgress: true,
			wantNoDrains:   true,
		},
		"no drain in progress with DRAINED pod does not auto-drain": {
			replicas: 2,
			pods: []*corev1.Pod{
				makePod(podName0, nil),
				makePod(podName1, nil),
			},
			podRoles: map[string]string{
				podName1: "DRAINED",
			},
			wantAction:   false,
			wantNoDrains: true,
		},
		"no drain in progress allows extra pod drain": {
			replicas: 1,
			pods: []*corev1.Pod{
				makePod(podName0, nil),
				makePod(podName1, nil),
			},
			wantAction:     true,
			wantInProgress: false,
			wantDrainedPod: podName1,
		},
		"actionTaken from earlier phase blocks DRAINED replacement": {
			replicas: 2,
			pods: []*corev1.Pod{
				makePod(podName0, nil),
				makePod(podName1, nil),
			},
			podRoles: map[string]string{
				podName1: "DRAINED",
			},
			actionTaken:    true,
			wantAction:     true,
			wantInProgress: false,
			wantNoDrains:   true,
		},
		"actionTaken from earlier phase blocks extra pod drain": {
			replicas: 1,
			pods: []*corev1.Pod{
				makePod(podName0, nil),
				makePod(podName1, nil),
			},
			actionTaken:    true,
			wantAction:     true,
			wantInProgress: false,
			wantNoDrains:   true,
		},
		"pod with DeletionTimestamp sets inProgress and blocks DRAINED replacement": {
			replicas: 2,
			pods: []*corev1.Pod{
				func() *corev1.Pod {
					p := makePod(podName0, nil)
					now := metav1.Now()
					p.DeletionTimestamp = &now
					p.Finalizers = []string{"kubernetes.io/test"}
					return p
				}(),
				makePod(podName1, nil),
			},
			podRoles: map[string]string{
				podName1: "DRAINED",
			},
			wantAction:     false,
			wantInProgress: true,
			wantNoDrains:   true,
		},
		"multiple DRAINED pods with drain in progress drains none": {
			replicas: 3,
			pods: []*corev1.Pod{
				makePod(podName0, map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateRequested,
				}),
				makePod(podName1, nil),
				makePod(podName2, nil),
			},
			podRoles: map[string]string{
				podName1: "DRAINED",
				podName2: "DRAINED",
			},
			wantAction:     false,
			wantInProgress: true,
			wantNoDrains:   true,
		},
		"ready-for-deletion pod is cleaned up even when other drain in progress": {
			replicas: 2,
			pods: []*corev1.Pod{
				makePod(podName0, map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateDraining,
				}),
				makePod(podName1, map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
				}),
			},
			wantAction:     true,
			wantInProgress: true,
			wantNoDrains:   true,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			shard := baseShard.DeepCopy()
			shard.Status.PodRoles = tc.podRoles

			objects := make([]client.Object, 0, len(tc.pods)+1)
			objects = append(objects, shard)
			for _, p := range tc.pods {
				objects = append(objects, p.DeepCopy())
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				Build()

			reconciler := &ShardReconciler{
				Client:   fakeClient,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(100),
			}

			existingPods := make(map[string]*corev1.Pod, len(tc.pods))
			for _, p := range tc.pods {
				existingPods[p.Name] = p
			}

			poolSpec := multigresv1alpha1.PoolSpec{}

			var drainedInTest int32
			for _, role := range tc.podRoles {
				if role == "DRAINED" {
					drainedInTest++
				}
			}
			effectiveReplicas := tc.replicas + drainedInTest

			gotAction, gotInProgress, err := reconciler.handleScaleDown(
				context.Background(),
				shard,
				poolName,
				poolSpec,
				existingPods,
				tc.replicas,
				effectiveReplicas,
				tc.actionTaken,
			)
			if err != nil {
				t.Fatalf("handleScaleDown() unexpected error: %v", err)
			}

			if gotAction != tc.wantAction {
				t.Errorf("actionTaken = %v, want %v", gotAction, tc.wantAction)
			}
			if gotInProgress != tc.wantInProgress {
				t.Errorf("inProgress = %v, want %v", gotInProgress, tc.wantInProgress)
			}

			for _, p := range tc.pods {
				updated := &corev1.Pod{}
				err := fakeClient.Get(
					context.Background(),
					client.ObjectKeyFromObject(p),
					updated,
				)
				if err != nil {
					// Pod may have been deleted by cleanupDrainedPod (ready-for-deletion flow)
					if errors.IsNotFound(err) {
						continue
					}
					t.Fatalf("failed to get pod %s: %v", p.Name, err)
				}

				drainState := updated.Annotations[metadata.AnnotationDrainState]
				originalState := p.Annotations[metadata.AnnotationDrainState]

				if tc.wantNoDrains && drainState != originalState {
					t.Errorf(
						"pod %s: drain state changed from %q to %q, expected no new drains",
						p.Name, originalState, drainState,
					)
				}

				if tc.wantDrainedPod == p.Name && drainState != metadata.DrainStateRequested {
					t.Errorf(
						"pod %s: drain state = %q, want %q",
						p.Name, drainState, metadata.DrainStateRequested,
					)
				}
			}
		})
	}
}

// TestSetupWithManager tests the manager setup function.
func TestSetupWithManager(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// dummy config
	cfg := &rest.Config{Host: "http://localhost:8080"}

	createMgr := func() ctrl.Manager {
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:  scheme,
			Metrics: metricsserver.Options{BindAddress: "0"},
		})
		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}
		return mgr
	}

	t.Run("default options", func(t *testing.T) {
		mgr := createMgr()
		r := &ShardReconciler{
			Client:    mgr.GetClient(),
			Scheme:    scheme,
			Recorder:  record.NewFakeRecorder(100),
			APIReader: mgr.GetClient(),
		}
		if err := r.SetupWithManager(mgr); err != nil {
			t.Errorf("SetupWithManager() error = %v", err)
		}
	})

	t.Run("with options", func(t *testing.T) {
		mgr := createMgr()
		r := &ShardReconciler{
			Client:    mgr.GetClient(),
			Scheme:    scheme,
			Recorder:  record.NewFakeRecorder(100),
			APIReader: mgr.GetClient(),
		}
		if err := r.SetupWithManager(mgr, controller.Options{
			MaxConcurrentReconciles: 1,
			SkipNameValidation:      ptr.To(true),
		}); err != nil {
			t.Errorf("SetupWithManager() with opts error = %v", err)
		}
	})
}

// TestCleanupDrainedPod_PVCDeletion verifies that cleanupDrainedPod handles
// PVC deletion correctly for DRAINED replacement pods (idx < replicas),
// scale-down pods (idx >= replicas), and rolling-update pods under different
// PVC deletion policies.
func TestCleanupDrainedPod_PVCDeletion(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "shard0",
		},
	}

	poolName := "primary"
	cellName := "zone1"
	replicas := int32(3)

	podName0 := BuildPoolPodName(baseShard, poolName, cellName, 0)
	pvcName0 := BuildPoolDataPVCName(baseShard, poolName, cellName, 0)
	podName1 := BuildPoolPodName(baseShard, poolName, cellName, 1)
	pvcName1 := BuildPoolDataPVCName(baseShard, poolName, cellName, 1)
	podName5 := BuildPoolPodName(baseShard, poolName, cellName, 5)
	pvcName5 := BuildPoolDataPVCName(baseShard, poolName, cellName, 5)

	makePod := func(n string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      n,
				Namespace: "default",
				Labels: map[string]string{
					metadata.LabelMultigresCell: cellName,
				},
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
				},
			},
		}
	}
	makePVC := func(n string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      n,
				Namespace: "default",
			},
		}
	}

	deletePolicy := &multigresv1alpha1.PVCDeletionPolicy{
		WhenScaled: multigresv1alpha1.DeletePVCRetentionPolicy,
	}
	retainPolicy := &multigresv1alpha1.PVCDeletionPolicy{
		WhenScaled: multigresv1alpha1.RetainPVCRetentionPolicy,
	}

	tests := map[string]struct {
		podName  string
		pvcName  string
		podRoles map[string]string
		policy   *multigresv1alpha1.PVCDeletionPolicy
		wantPVC  bool
	}{
		"DRAINED replacement (idx<replicas) with Delete policy deletes PVC": {
			podName:  podName0,
			pvcName:  pvcName0,
			podRoles: map[string]string{podName0: "DRAINED"},
			policy:   deletePolicy,
			wantPVC:  false,
		},
		"DRAINED replacement (idx<replicas) with Retain policy deletes PVC": {
			podName:  podName1,
			pvcName:  pvcName1,
			podRoles: map[string]string{podName1: "DRAINED"},
			policy:   retainPolicy,
			wantPVC:  false,
		},
		"non-DRAINED pod (idx<replicas) with Delete policy keeps PVC": {
			podName:  podName0,
			pvcName:  pvcName0,
			podRoles: map[string]string{podName0: "REPLICA"},
			policy:   deletePolicy,
			wantPVC:  true,
		},
		"scale-down (idx>=replicas) with Delete policy deletes PVC": {
			podName:  podName5,
			pvcName:  pvcName5,
			podRoles: map[string]string{},
			policy:   deletePolicy,
			wantPVC:  false,
		},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			shard := baseShard.DeepCopy()
			shard.Status.PodRoles = tc.podRoles

			pod := makePod(tc.podName)
			if tc.podRoles[tc.podName] == "DRAINED" {
				pod.Labels[metadata.LabelPodRole] = "DRAINED"
			}
			pvc := makePVC(tc.pvcName)

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(shard, pod, pvc).
				Build()

			reconciler := &ShardReconciler{
				Client:    fakeClient,
				Scheme:    scheme,
				Recorder:  record.NewFakeRecorder(100),
				APIReader: fakeClient,
			}

			poolSpec := multigresv1alpha1.PoolSpec{
				PVCDeletionPolicy: tc.policy,
			}

			err := reconciler.cleanupDrainedPod(
				context.Background(), shard, pod, poolName, poolSpec, replicas,
			)
			if err != nil {
				t.Fatalf("cleanupDrainedPod() returned unexpected error: %v", err)
			}

			pvcAfter := &corev1.PersistentVolumeClaim{}
			getErr := fakeClient.Get(
				context.Background(),
				client.ObjectKey{Namespace: "default", Name: tc.pvcName},
				pvcAfter,
			)
			pvcExists := getErr == nil
			if pvcExists != tc.wantPVC {
				t.Errorf(
					"PVC %s exists = %v, want %v (err=%v)",
					tc.pvcName,
					pvcExists,
					tc.wantPVC,
					getErr,
				)
			}

			podAfter := &corev1.Pod{}
			if err := fakeClient.Get(
				context.Background(),
				client.ObjectKey{Namespace: "default", Name: tc.podName},
				podAfter,
			); err != nil {
				t.Fatalf("failed to get pod after cleanup: %v", err)
			}
		})
	}
}

func TestHandleExternalDeletion(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}

	t.Run("unscheduled pod is ignored", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-unsched",
				Namespace: "default",
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionFalse},
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.handleExternalDeletion(context.Background(), shard, pod); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod),
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
	})

	t.Run("scheduled pod without drain annotation gets drain initiated", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "pod-sched",
				Namespace:   "default",
				Annotations: map[string]string{},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		rec := record.NewFakeRecorder(10)
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: rec}

		if err := r.handleExternalDeletion(context.Background(), shard, pod); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod),
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updated.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
			t.Errorf("drain state = %q, want %q",
				updated.Annotations[metadata.AnnotationDrainState], metadata.DrainStateRequested)
		}

		select {
		case event := <-rec.Events:
			if !strings.Contains(event, "ExternalDeletion") {
				t.Errorf("expected ExternalDeletion event, got %q", event)
			}
		default:
			t.Error("expected ExternalDeletion event")
		}
	})

	t.Run("scheduled pod with existing drain annotation is left alone", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-draining",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateDraining,
				},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.handleExternalDeletion(context.Background(), shard, pod); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod),
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updated.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateDraining {
			t.Errorf("drain state changed to %q, should remain %q",
				updated.Annotations[metadata.AnnotationDrainState], metadata.DrainStateDraining)
		}
	})

	t.Run("unscheduled pod without scheduled condition is ignored", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-no-conditions",
				Namespace: "default",
			},
			Status: corev1.PodStatus{},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.handleExternalDeletion(context.Background(), shard, pod); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod),
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
	})

	t.Run("error initiating drain for scheduled pod", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "pod-fail-drain",
				Namespace:   "default",
				Annotations: map[string]string{},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				},
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnPatch: func(obj client.Object) error {
				if obj.GetName() == "pod-fail-drain" {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.handleExternalDeletion(context.Background(), shard, pod)
		if err == nil {
			t.Error("expected error when initiateDrain fails")
		}
	})
}

func TestReconcilePgBackRestCerts(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	t.Run("nil backup config returns nil", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
			Spec:       multigresv1alpha1.ShardSpec{},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ShardReconciler{
			Client:    c,
			Scheme:    scheme,
			Recorder:  record.NewFakeRecorder(10),
			APIReader: c,
		}

		if err := r.reconcilePgBackRestCerts(context.Background(), shard); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("user-provided secret with valid keys succeeds", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-shard", Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{
						SecretName: "my-tls-secret",
					},
				},
			},
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "my-tls-secret", Namespace: "default"},
			Data: map[string][]byte{
				"ca.crt":  []byte("ca-cert"),
				"tls.crt": []byte("tls-cert"),
				"tls.key": []byte("tls-key"),
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, secret).Build()
		r := &ShardReconciler{
			Client:    c,
			Scheme:    scheme,
			Recorder:  record.NewFakeRecorder(10),
			APIReader: c,
		}

		if err := r.reconcilePgBackRestCerts(context.Background(), shard); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("user-provided secret not found returns error", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-shard", Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{
						SecretName: "missing-secret",
					},
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		r := &ShardReconciler{
			Client:    c,
			Scheme:    scheme,
			Recorder:  record.NewFakeRecorder(10),
			APIReader: c,
		}

		err := r.reconcilePgBackRestCerts(context.Background(), shard)
		if err == nil {
			t.Error("expected error for missing secret")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("expected 'not found' error, got: %v", err)
		}
	})

	t.Run("user-provided secret missing required key returns error", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-shard", Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{
						SecretName: "partial-secret",
					},
				},
			},
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "partial-secret", Namespace: "default"},
			Data: map[string][]byte{
				"ca.crt":  []byte("ca-cert"),
				"tls.crt": []byte("tls-cert"),
				// missing tls.key
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, secret).Build()
		r := &ShardReconciler{
			Client:    c,
			Scheme:    scheme,
			Recorder:  record.NewFakeRecorder(10),
			APIReader: c,
		}

		err := r.reconcilePgBackRestCerts(context.Background(), shard)
		if err == nil {
			t.Error("expected error for missing key")
		}
		if !strings.Contains(err.Error(), "tls.key") {
			t.Errorf("expected error about missing 'tls.key', got: %v", err)
		}
	})
}

func TestCreateMissingResources(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}

	poolSpec := multigresv1alpha1.PoolSpec{
		Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
	}
	poolName := "primary"
	cellName := "zone1"

	t.Run("terminal pod (Failed) is deleted for recreation", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 0)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
			},
			Status: corev1.PodStatus{Phase: corev1.PodFailed},
		}
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "default"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, pvc).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := map[string]*corev1.Pod{podName: pod}
		existingPVCs := map[string]*corev1.PersistentVolumeClaim{pvcName: pvc}

		_, actionTaken, err := r.createMissingResources(
			context.Background(),
			shard,
			poolName,
			cellName,
			poolSpec,
			existingPods,
			existingPVCs,
			1,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !actionTaken {
			t.Error("expected actionTaken for terminal pod deletion")
		}

		// Pod should be deleted
		err = c.Get(
			context.Background(),
			types.NamespacedName{Name: podName, Namespace: "default"},
			&corev1.Pod{},
		)
		if !errors.IsNotFound(err) {
			t.Errorf("terminal pod should be deleted, but Get returned: %v", err)
		}
	})

	t.Run("terminal pod (Succeeded) is deleted for recreation", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 0)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: "default"},
			Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
		}
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "default"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, pvc).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := map[string]*corev1.Pod{podName: pod}
		existingPVCs := map[string]*corev1.PersistentVolumeClaim{pvcName: pvc}

		_, actionTaken, err := r.createMissingResources(
			context.Background(),
			shard,
			poolName,
			cellName,
			poolSpec,
			existingPods,
			existingPVCs,
			1,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !actionTaken {
			t.Error("expected actionTaken for terminal pod deletion")
		}
	})

	t.Run(
		"externally deleted pod (DeletionTimestamp + finalizer) calls handleExternalDeletion",
		func(t *testing.T) {
			shard := baseShard.DeepCopy()
			podName := BuildPoolPodName(shard, poolName, cellName, 0)
			pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 0)

			now := metav1.Now()
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:              podName,
					Namespace:         "default",
					DeletionTimestamp: &now,
					Finalizers:        []string{"kubernetes.io/test"},
					Annotations:       map[string]string{},
				},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
					},
				},
			}
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "default"},
			}

			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, pvc).Build()
			r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

			existingPods := map[string]*corev1.Pod{podName: pod}
			existingPVCs := map[string]*corev1.PersistentVolumeClaim{pvcName: pvc}

			_, actionTaken, err := r.createMissingResources(
				context.Background(),
				shard,
				poolName,
				cellName,
				poolSpec,
				existingPods,
				existingPVCs,
				1,
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !actionTaken {
				t.Error("expected actionTaken for externally deleted pod")
			}

			// Should have initiated drain
			updated := &corev1.Pod{}
			if err := c.Get(
				context.Background(),
				client.ObjectKeyFromObject(pod),
				updated,
			); err != nil {
				t.Fatalf("failed to get pod: %v", err)
			}
			if updated.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
				t.Errorf(
					"drain state = %q, want %q",
					updated.Annotations[metadata.AnnotationDrainState],
					metadata.DrainStateRequested,
				)
			}
		},
	)

	t.Run("not-ready pod does not block creation of other replicas", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName0 := BuildPoolPodName(shard, poolName, cellName, 0)
		podName1 := BuildPoolPodName(shard, poolName, cellName, 1)
		pvcName0 := BuildPoolDataPVCName(shard, poolName, cellName, 0)
		pvcName1 := BuildPoolDataPVCName(shard, poolName, cellName, 1)

		desiredPod0, _ := BuildPoolPod(shard, poolName, cellName, poolSpec, 0, scheme)
		hash0 := ComputeSpecHash(desiredPod0)

		// Pod 0: not ready, has matching spec hash (not drifted, not DRAINED)
		pod0 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName0,
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationSpecHash: hash0,
				},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				},
			},
		}
		pvc0 := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName0, Namespace: "default"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod0, pvc0).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := map[string]*corev1.Pod{podName0: pod0}
		existingPVCs := map[string]*corev1.PersistentVolumeClaim{pvcName0: pvc0}

		_, actionTaken, err := r.createMissingResources(
			context.Background(),
			shard,
			poolName,
			cellName,
			poolSpec,
			existingPods,
			existingPVCs,
			2,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !actionTaken {
			t.Error("expected actionTaken for pod creation")
		}

		// Pod 1 SHOULD be created even though pod 0 is not ready
		if err := c.Get(
			context.Background(),
			types.NamespacedName{Name: podName1, Namespace: "default"},
			&corev1.Pod{},
		); err != nil {
			t.Errorf("pod-1 should have been created despite pod-0 being not ready: %v", err)
		}
		if err := c.Get(
			context.Background(),
			types.NamespacedName{Name: pvcName1, Namespace: "default"},
			&corev1.PersistentVolumeClaim{},
		); err != nil {
			t.Errorf("pvc-1 should have been created despite pod-0 being not ready: %v", err)
		}
	})

	t.Run("all missing pods and PVCs created in one pass", func(t *testing.T) {
		shard := baseShard.DeepCopy()

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		_, actionTaken, err := r.createMissingResources(
			context.Background(),
			shard,
			poolName,
			cellName,
			poolSpec,
			map[string]*corev1.Pod{},
			map[string]*corev1.PersistentVolumeClaim{},
			3,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !actionTaken {
			t.Error("expected actionTaken for pod creation")
		}

		for i := 0; i < 3; i++ {
			podName := BuildPoolPodName(shard, poolName, cellName, i)
			pvcName := BuildPoolDataPVCName(shard, poolName, cellName, i)
			if err := c.Get(
				context.Background(),
				types.NamespacedName{Name: podName, Namespace: "default"},
				&corev1.Pod{},
			); err != nil {
				t.Errorf("pod-%d should exist: %v", i, err)
			}
			if err := c.Get(
				context.Background(),
				types.NamespacedName{Name: pvcName, Namespace: "default"},
				&corev1.PersistentVolumeClaim{},
			); err != nil {
				t.Errorf("pvc-%d should exist: %v", i, err)
			}
		}
	})

	t.Run("actionTaken blocks terminal pod deletion", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName0 := BuildPoolPodName(shard, poolName, cellName, 0)
		podName1 := BuildPoolPodName(shard, poolName, cellName, 1)
		pvcName0 := BuildPoolDataPVCName(shard, poolName, cellName, 0)
		pvcName1 := BuildPoolDataPVCName(shard, poolName, cellName, 1)

		pod0 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName0, Namespace: "default"},
			Status:     corev1.PodStatus{Phase: corev1.PodFailed},
		}
		pod1 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName1, Namespace: "default"},
			Status:     corev1.PodStatus{Phase: corev1.PodFailed},
		}
		pvc0 := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName0, Namespace: "default"},
		}
		pvc1 := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName1, Namespace: "default"},
		}

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod0, pod1, pvc0, pvc1).
			Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := map[string]*corev1.Pod{podName0: pod0, podName1: pod1}
		existingPVCs := map[string]*corev1.PersistentVolumeClaim{pvcName0: pvc0, pvcName1: pvc1}

		_, _, err := r.createMissingResources(
			context.Background(),
			shard,
			poolName,
			cellName,
			poolSpec,
			existingPods,
			existingPVCs,
			2,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Only one of the two terminal pods should have been deleted (actionTaken blocks second)
		deleted := 0
		for _, n := range []string{podName0, podName1} {
			if err := c.Get(
				context.Background(),
				types.NamespacedName{Name: n, Namespace: "default"},
				&corev1.Pod{},
			); errors.IsNotFound(
				err,
			) {
				deleted++
			}
		}
		if deleted != 1 {
			t.Errorf("expected exactly 1 terminal pod deleted (sequential gating), got %d", deleted)
		}
	})

	t.Run("missing pod is created", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 0)

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := map[string]*corev1.Pod{}
		existingPVCs := map[string]*corev1.PersistentVolumeClaim{}

		_, actionTaken, err := r.createMissingResources(
			context.Background(),
			shard,
			poolName,
			cellName,
			poolSpec,
			existingPods,
			existingPVCs,
			1,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !actionTaken {
			t.Error("expected actionTaken for pod creation")
		}

		// Pod and PVC should exist
		if err := c.Get(
			context.Background(),
			types.NamespacedName{Name: podName, Namespace: "default"},
			&corev1.Pod{},
		); err != nil {
			t.Errorf("pod should exist: %v", err)
		}
		if err := c.Get(
			context.Background(),
			types.NamespacedName{Name: pvcName, Namespace: "default"},
			&corev1.PersistentVolumeClaim{},
		); err != nil {
			t.Errorf("PVC should exist: %v", err)
		}
	})

	t.Run("error creating pod", func(t *testing.T) {
		shard := baseShard.DeepCopy()

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnCreate: func(obj client.Object) error {
				if _, ok := obj.(*corev1.Pod); ok {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		_, _, err := r.createMissingResources(
			context.Background(), shard, poolName, cellName, poolSpec,
			map[string]*corev1.Pod{}, map[string]*corev1.PersistentVolumeClaim{}, 1,
		)
		if err == nil {
			t.Error("expected error on pod create failure")
		}
	})

	t.Run("error creating PVC", func(t *testing.T) {
		shard := baseShard.DeepCopy()

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnCreate: func(obj client.Object) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		_, _, err := r.createMissingResources(
			context.Background(), shard, poolName, cellName, poolSpec,
			map[string]*corev1.Pod{}, map[string]*corev1.PersistentVolumeClaim{}, 1,
		)
		if err == nil {
			t.Error("expected error on PVC create failure")
		}
	})

	t.Run("error deleting terminal pod", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 0)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: "default"},
			Status:     corev1.PodStatus{Phase: corev1.PodFailed},
		}
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "default"},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, pvc).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnDelete: func(obj client.Object) error {
				return testutil.ErrNetworkTimeout
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := map[string]*corev1.Pod{podName: pod}
		existingPVCs := map[string]*corev1.PersistentVolumeClaim{pvcName: pvc}

		_, _, err := r.createMissingResources(
			context.Background(),
			shard,
			poolName,
			cellName,
			poolSpec,
			existingPods,
			existingPVCs,
			1,
		)
		if err == nil {
			t.Error("expected error on terminal pod delete failure")
		}
	})
}

func TestResolvePodIndex(t *testing.T) {
	tests := map[string]struct {
		podName string
		want    int
		wantOK  bool
	}{
		"normal pod name": {
			podName: "test-cluster-db-tg-s1-pool-primary-zone1-3",
			want:    3,
			wantOK:  true,
		},
		"zero index":        {podName: "pod-0", want: 0, wantOK: true},
		"no dash":           {podName: "nodash", want: 0, wantOK: false},
		"non-numeric after": {podName: "pod-abc", want: 0, wantOK: false},
		"trailing dash":     {podName: "pod-", want: 0, wantOK: false},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			got, ok := resolvePodIndex(tc.podName)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf(
					"resolvePodIndex(%q) = (%d, %v), want (%d, %v)",
					tc.podName,
					got,
					ok,
					tc.want,
					tc.wantOK,
				)
			}
		})
	}
}

func TestIsPodReady(t *testing.T) {
	t.Run("nil pod", func(t *testing.T) {
		if isPodReady(nil) {
			t.Error("expected false for nil pod")
		}
	})

	t.Run("no conditions", func(t *testing.T) {
		pod := &corev1.Pod{}
		if isPodReady(pod) {
			t.Error("expected false for pod with no conditions")
		}
	})

	t.Run("ready condition false", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				},
			},
		}
		if isPodReady(pod) {
			t.Error("expected false for pod with PodReady=False")
		}
	})

	t.Run("ready condition true", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		if !isPodReady(pod) {
			t.Error("expected true for pod with PodReady=True")
		}
	})

	t.Run("only non-ready conditions", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
					{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
				},
			},
		}
		if isPodReady(pod) {
			t.Error("expected false for pod with no PodReady condition")
		}
	})
}

func TestIsPoolHealthy(t *testing.T) {
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
		},
	}

	t.Run("empty pool is healthy", func(t *testing.T) {
		if !isPoolHealthy(map[string]*corev1.Pod{}, 1, shard) {
			t.Error("expected empty pool to be healthy")
		}
	})

	t.Run("draining pod is excluded from health check", func(t *testing.T) {
		pods := map[string]*corev1.Pod{
			"pod-0": {
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod-0",
					Annotations: map[string]string{
						metadata.AnnotationDrainState: metadata.DrainStateRequested,
					},
				},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionFalse},
					},
				},
			},
		}
		if !isPoolHealthy(pods, 1, shard) {
			t.Error("draining pod should be excluded from health check")
		}
	})

	t.Run("pod being deleted is excluded from health check", func(t *testing.T) {
		now := metav1.Now()
		pods := map[string]*corev1.Pod{
			"pod-0": {
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-0",
					DeletionTimestamp: &now,
					Finalizers:        []string{"kubernetes.io/test"},
				},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionFalse},
					},
				},
			},
		}
		if !isPoolHealthy(pods, 1, shard) {
			t.Error("terminating pod should be excluded from health check")
		}
	})

	t.Run("extra pod (index >= replicas) is excluded from health check", func(t *testing.T) {
		pods := map[string]*corev1.Pod{
			"pod-0": {
				ObjectMeta: metav1.ObjectMeta{Name: "pod-0"},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			},
			"pod-2": {
				ObjectMeta: metav1.ObjectMeta{Name: "pod-2"},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionFalse},
					},
				},
			},
		}
		if !isPoolHealthy(pods, 1, shard) {
			t.Error("extra pod at index 2 with replicas=1 should be excluded from health check")
		}
	})

	t.Run("DRAINED pod that is not ready does not block health check", func(t *testing.T) {
		drainedShard := shard.DeepCopy()
		drainedShard.Status.PodRoles = map[string]string{"pod-0": "DRAINED"}
		pods := map[string]*corev1.Pod{
			"pod-0": {
				ObjectMeta: metav1.ObjectMeta{Name: "pod-0"},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionFalse},
					},
				},
			},
		}
		if !isPoolHealthy(pods, 1, drainedShard) {
			t.Error("DRAINED pod should be excluded from health check")
		}
	})
}

func TestPodNeedsUpdate(t *testing.T) {
	shard := newTestShard()
	pool := newTestPoolSpec()
	s := testScheme()

	t.Run("pod with deletion timestamp does not need update", func(t *testing.T) {
		now := metav1.Now()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				DeletionTimestamp: &now,
				Finalizers:        []string{"kubernetes.io/test"},
				Annotations:       map[string]string{metadata.AnnotationSpecHash: "old"},
			},
		}
		if podNeedsUpdate(pod, shard, "main", "z1", pool, 0, s) {
			t.Error("pod with deletion timestamp should not need update")
		}
	})

	t.Run("pod missing spec-hash annotation needs update", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{},
		}
		if !podNeedsUpdate(pod, shard, "main", "z1", pool, 0, s) {
			t.Error("pod without spec-hash annotation should need update")
		}
	})

	t.Run("pod with matching spec-hash does not need update", func(t *testing.T) {
		desired, _ := BuildPoolPod(shard, "main", "z1", pool, 0, s)
		hash := ComputeSpecHash(desired)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{metadata.AnnotationSpecHash: hash},
			},
		}
		if podNeedsUpdate(pod, shard, "main", "z1", pool, 0, s) {
			t.Error("pod with matching spec-hash should not need update")
		}
	})

	t.Run("pod with old spec-hash needs update", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{metadata.AnnotationSpecHash: "old-hash"},
			},
		}
		if !podNeedsUpdate(pod, shard, "main", "z1", pool, 0, s) {
			t.Error("pod with old spec-hash should need update")
		}
	})

	t.Run("build error assumes no update needed", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{metadata.AnnotationSpecHash: "some-hash"},
			},
		}
		if podNeedsUpdate(pod, shard, "main", "z1", pool, 0, emptyScheme) {
			t.Error("build failure should assume no update needed")
		}
	})
}

func TestInitiateDrain(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	t.Run("sets drain annotations on pod with nil annotations", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-nil-ann",
				Namespace: "default",
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.initiateDrain(context.Background(), pod); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod),
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updated.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
			t.Errorf("drain state = %q, want %q",
				updated.Annotations[metadata.AnnotationDrainState], metadata.DrainStateRequested)
		}
		if updated.Annotations[metadata.AnnotationDrainRequestedAt] == "" {
			t.Error("drain requested-at timestamp should be set")
		}
	})

	t.Run("error on patch failure", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-patch-fail",
				Namespace: "default",
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnPatch: func(obj client.Object) error {
				return testutil.ErrNetworkTimeout
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.initiateDrain(context.Background(), pod)
		if err == nil {
			t.Error("expected error on patch failure")
		}
	})
}

func TestHandleRollingUpdates(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	poolName := "primary"
	cellName := "zone1"

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}

	poolSpec := multigresv1alpha1.PoolSpec{
		Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
	}

	t.Run("no drifted pods sets RollingUpdate condition to false", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.handleRollingUpdates(
			context.Background(), shard, poolName, cellName, poolSpec,
			map[string]*corev1.Pod{}, 0, false, false,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		found := false
		for _, cond := range shard.Status.Conditions {
			if cond.Type == "RollingUpdate" {
				found = true
				if cond.Status != metav1.ConditionFalse {
					t.Errorf("RollingUpdate condition status = %s, want False", cond.Status)
				}
			}
		}
		if !found {
			t.Error("RollingUpdate condition not set")
		}
	})

	t.Run("actionTaken skips drain initiation", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationSpecHash: "old-hash",
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.handleRollingUpdates(
			context.Background(), shard, poolName, cellName, poolSpec,
			map[string]*corev1.Pod{podName: pod}, 1, true, false,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Pod should NOT have drain annotation (actionTaken blocks)
		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod),
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updated.Annotations[metadata.AnnotationDrainState] != "" {
			t.Error("drain annotation should not be set when actionTaken is true")
		}
	})

	t.Run("isAnyPodDraining skips drain initiation", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationSpecHash: "old-hash",
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.handleRollingUpdates(
			context.Background(), shard, poolName, cellName, poolSpec,
			map[string]*corev1.Pod{podName: pod}, 1, false, true,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod),
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updated.Annotations[metadata.AnnotationDrainState] != "" {
			t.Error("drain annotation should not be set when isAnyPodDraining is true")
		}
	})

	t.Run("primary-only drift initiates drain for primary", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		shard.Status.PodRoles = map[string]string{podName: "PRIMARY"}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationSpecHash: "old-hash",
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.handleRollingUpdates(
			context.Background(), shard, poolName, cellName, poolSpec,
			map[string]*corev1.Pod{podName: pod}, 1, false, false,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod),
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updated.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
			t.Errorf("primary pod should have drain requested, got %q",
				updated.Annotations[metadata.AnnotationDrainState])
		}
	})

	t.Run("primary with existing drain annotation is skipped", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		shard.Status.PodRoles = map[string]string{podName: "PRIMARY"}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationSpecHash:   "old-hash",
					metadata.AnnotationDrainState: metadata.DrainStateDraining,
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.handleRollingUpdates(
			context.Background(), shard, poolName, cellName, poolSpec,
			map[string]*corev1.Pod{podName: pod}, 1, false, false,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod),
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		// Should still be "draining", not changed to "requested"
		if updated.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateDraining {
			t.Errorf("primary pod drain state should remain %q, got %q",
				metadata.DrainStateDraining, updated.Annotations[metadata.AnnotationDrainState])
		}
	})

	t.Run("replica is drained before primary", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName0 := BuildPoolPodName(shard, poolName, cellName, 0)
		podName1 := BuildPoolPodName(shard, poolName, cellName, 1)
		shard.Status.PodRoles = map[string]string{
			podName0: "REPLICA",
			podName1: "PRIMARY",
		}

		pod0 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName0, Namespace: "default",
				Annotations: map[string]string{metadata.AnnotationSpecHash: "old-hash"},
			},
		}
		pod1 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName1, Namespace: "default",
				Annotations: map[string]string{metadata.AnnotationSpecHash: "old-hash"},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod0, pod1).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.handleRollingUpdates(
			context.Background(), shard, poolName, cellName, poolSpec,
			map[string]*corev1.Pod{podName0: pod0, podName1: pod1}, 2, false, false,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Replica should have drain requested
		updated0 := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod0),
			updated0,
		); err != nil {
			t.Fatalf("failed to get pod0: %v", err)
		}
		if updated0.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
			t.Errorf("replica pod should have drain requested, got %q",
				updated0.Annotations[metadata.AnnotationDrainState])
		}

		// Primary should NOT have drain annotation (replica first)
		updated1 := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			client.ObjectKeyFromObject(pod1),
			updated1,
		); err != nil {
			t.Fatalf("failed to get pod1: %v", err)
		}
		if updated1.Annotations[metadata.AnnotationDrainState] != "" {
			t.Errorf("primary pod should NOT have drain annotation yet, got %q",
				updated1.Annotations[metadata.AnnotationDrainState])
		}
	})

	t.Run("error initiating drain for replica", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		shard.Status.PodRoles = map[string]string{podName: "REPLICA"}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName, Namespace: "default",
				Annotations: map[string]string{metadata.AnnotationSpecHash: "old-hash"},
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnPatch: func(obj client.Object) error {
				return testutil.ErrNetworkTimeout
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.handleRollingUpdates(
			context.Background(), shard, poolName, cellName, poolSpec,
			map[string]*corev1.Pod{podName: pod}, 1, false, false,
		)
		if err == nil {
			t.Error("expected error when drain initiation fails")
		}
	})

	t.Run("error initiating drain for primary", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		shard.Status.PodRoles = map[string]string{podName: "PRIMARY"}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName, Namespace: "default",
				Annotations: map[string]string{metadata.AnnotationSpecHash: "old-hash"},
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnPatch: func(obj client.Object) error {
				return testutil.ErrNetworkTimeout
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.handleRollingUpdates(
			context.Background(), shard, poolName, cellName, poolSpec,
			map[string]*corev1.Pod{podName: pod}, 1, false, false,
		)
		if err == nil {
			t.Error("expected error when primary drain initiation fails")
		}
	})
}

func TestReconcileSharedBackupPVC(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	t.Run("S3 backup type skips PVC creation", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-shard", Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeS3,
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.reconcileSharedBackupPVC(context.Background(), shard, "zone1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("nil backup creates PVC with defaults", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-shard", Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "db",
				TableGroupName: "tg",
				ShardName:      "s1",
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.reconcileSharedBackupPVC(context.Background(), shard, "zone1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		pvcName := BuildSharedBackupPVCName(shard, "zone1")
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(
			context.Background(),
			types.NamespacedName{Name: pvcName, Namespace: "default"},
			pvc,
		); err != nil {
			t.Fatalf("PVC should exist: %v", err)
		}
	})

	t.Run("error on patch failure", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-shard", Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "db",
				TableGroupName: "tg",
				ShardName:      "s1",
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnPatch: func(obj client.Object) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.reconcileSharedBackupPVC(context.Background(), shard, "zone1")
		if err == nil {
			t.Error("expected error on PVC patch failure")
		}
	})
}

func TestBuildSharedBackupPVC_Variants(t *testing.T) {
	t.Run("filesystem backup with custom storage class", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-shard", Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "db",
				TableGroupName: "tg",
				ShardName:      "s1",
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
						Storage: multigresv1alpha1.StorageSpec{
							Size:        "100Gi",
							Class:       "premium-ssd",
							AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
						},
					},
				},
			},
		}

		pvc, err := BuildSharedBackupPVC(shard, "zone1", false, testScheme())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pvc == nil {
			t.Fatal("expected non-nil PVC")
		}
		if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "premium-ssd" {
			t.Errorf("storage class = %v, want premium-ssd", pvc.Spec.StorageClassName)
		}
		if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteMany {
			t.Errorf("access modes = %v, want [ReadWriteMany]", pvc.Spec.AccessModes)
		}
	})

	t.Run("nil filesystem config uses defaults", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-shard", Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "db",
				TableGroupName: "tg",
				ShardName:      "s1",
				Backup: &multigresv1alpha1.BackupConfig{
					Type:       multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: nil,
				},
			},
		}

		pvc, err := BuildSharedBackupPVC(shard, "zone1", false, testScheme())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pvc == nil {
			t.Fatal("expected non-nil PVC")
		}
		if pvc.Spec.StorageClassName != nil {
			t.Errorf("storage class should be nil by default, got %v", pvc.Spec.StorageClassName)
		}
	})
}

func TestCleanupDrainedPod_ErrorPaths(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}
	poolName := "primary"
	cellName := "zone1"

	t.Run("error getting PVC for deletion", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 5) // index >= replicas
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName, Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCell: cellName},
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
				},
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnGet: func(key client.ObjectKey) error {
				if strings.Contains(key.Name, "data-") {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		poolSpec := multigresv1alpha1.PoolSpec{
			PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenScaled: multigresv1alpha1.DeletePVCRetentionPolicy,
			},
		}

		err := r.cleanupDrainedPod(context.Background(), shard, pod, poolName, poolSpec, 3)
		if err == nil {
			t.Error("expected error on PVC Get failure")
		}
	})

	t.Run("error deleting PVC", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 5)
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 5)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName, Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCell: cellName},
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
				},
			},
		}
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "default"},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, pvc).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnDelete: func(obj client.Object) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		poolSpec := multigresv1alpha1.PoolSpec{
			PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenScaled: multigresv1alpha1.DeletePVCRetentionPolicy,
			},
		}

		err := r.cleanupDrainedPod(context.Background(), shard, pod, poolName, poolSpec, 3)
		if err == nil {
			t.Error("expected error on PVC delete failure")
		}
	})

	t.Run("nil PVC deletion policy deletes PVC", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 5)
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 5)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName, Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCell: cellName},
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
				},
			},
		}
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "default"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, pvc).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		// nil PVCDeletionPolicy defaults to Delete
		err := r.cleanupDrainedPod(
			context.Background(),
			shard,
			pod,
			poolName,
			multigresv1alpha1.PoolSpec{},
			3,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// PVC should be deleted
		if err := c.Get(
			context.Background(),
			types.NamespacedName{Name: pvcName, Namespace: "default"},
			&corev1.PersistentVolumeClaim{},
		); !errors.IsNotFound(err) {
			t.Errorf("PVC should be deleted with nil policy, got err: %v", err)
		}
	})
}

func TestReconcilePoolPods_ErrorPaths(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}

	poolSpec := multigresv1alpha1.PoolSpec{
		ReplicasPerCell: ptr.To(int32(1)),
		Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
	}

	t.Run("error listing pods", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnList: func(list client.ObjectList) error {
				if _, ok := list.(*corev1.PodList); ok {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.reconcilePoolPods(context.Background(), shard, "primary", "zone1", poolSpec)
		if err == nil {
			t.Error("expected error on pod list failure")
		}
	})

	t.Run("error listing PVCs", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnList: func(list client.ObjectList) error {
				if _, ok := list.(*corev1.PersistentVolumeClaimList); ok {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		err := r.reconcilePoolPods(context.Background(), shard, "primary", "zone1", poolSpec)
		if err == nil {
			t.Error("expected error on PVC list failure")
		}
	})
}

func TestHandleScaleDown_ErrorPaths(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	poolName := "primary"
	cellName := "zone1"

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}

	t.Run("error draining extra pod", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName0 := BuildPoolPodName(shard, poolName, cellName, 0)
		podName1 := BuildPoolPodName(shard, poolName, cellName, 1)

		pod0 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName0, Namespace: "default",
				Annotations: map[string]string{},
				Labels:      map[string]string{metadata.LabelMultigresCell: cellName},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		pod1 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName1, Namespace: "default",
				Annotations: map[string]string{},
				Labels:      map[string]string{metadata.LabelMultigresCell: cellName},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod0, pod1).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnPatch: func(obj client.Object) error {
				return testutil.ErrNetworkTimeout
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := map[string]*corev1.Pod{podName0: pod0, podName1: pod1}
		_, _, err := r.handleScaleDown(
			context.Background(), shard, poolName,
			multigresv1alpha1.PoolSpec{}, existingPods, 1, 1, false,
		)
		if err == nil {
			t.Error("expected error on drain initiation failure for extra pod")
		}
	})

	t.Run("error deleting ready-for-deletion pod after cleanup", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName0 := BuildPoolPodName(shard, poolName, cellName, 0)

		// Pod without finalizer (so cleanup succeeds but Delete fails)
		pod0 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName0, Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
				},
				Labels: map[string]string{metadata.LabelMultigresCell: cellName},
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod0).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnDelete: func(obj client.Object) error {
				if p, ok := obj.(*corev1.Pod); ok && p.Name == podName0 {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := map[string]*corev1.Pod{podName0: pod0}
		_, _, err := r.handleScaleDown(
			context.Background(), shard, poolName,
			multigresv1alpha1.PoolSpec{}, existingPods, 1, 1, false,
		)
		if err == nil {
			t.Error("expected error on pod delete failure after cleanup")
		}
	})

	t.Run("error handling external deletion of extra pod", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName1 := BuildPoolPodName(shard, poolName, cellName, 1)

		now := metav1.Now()
		pod1 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName1, Namespace: "default",
				Annotations:       map[string]string{},
				Labels:            map[string]string{metadata.LabelMultigresCell: cellName},
				DeletionTimestamp: &now,
				Finalizers:        []string{"kubernetes.io/test"},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				},
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod1).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnPatch: func(obj client.Object) error {
				return testutil.ErrNetworkTimeout
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := map[string]*corev1.Pod{podName1: pod1}
		_, _, err := r.handleScaleDown(
			context.Background(), shard, poolName,
			multigresv1alpha1.PoolSpec{}, existingPods, 1, 1, false,
		)
		if err == nil {
			t.Error("expected error on external deletion handling failure")
		}
	})
}

func TestSelectPodToDrain_NilPod(t *testing.T) {
	r := &ShardReconciler{}
	shard := &multigresv1alpha1.Shard{}

	t.Run("empty list returns nil", func(t *testing.T) {
		result := r.selectPodToDrain(context.Background(), []*corev1.Pod{}, shard)
		if result != nil {
			t.Errorf("expected nil for empty list, got %v", result)
		}
	})

	t.Run("nil entries are skipped", func(t *testing.T) {
		pods := []*corev1.Pod{
			nil,
			{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-0"},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			},
		}
		result := r.selectPodToDrain(context.Background(), pods, shard)
		if result == nil || result.Name != "pod-0" {
			t.Errorf("expected pod-0, got %v", result)
		}
	})
}

func TestReconcile_BackupCertsError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-cert-err",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			Backup: &multigresv1alpha1.BackupConfig{
				PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{
					SecretName: "nonexistent-tls-secret",
				},
			},
			MultiOrch: multigresv1alpha1.MultiOrchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells: []multigresv1alpha1.CellName{"zone1"},
				},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	reconciler := &ShardReconciler{
		Client:          c,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(100),
		APIReader:       c,
		CreateTopoStore: newMemoryTopoFactory(),
	}

	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(shard)}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err == nil {
		t.Error("expected error when pgBackRest TLS secret not found")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' error, got: %v", err)
	}
}

func TestResolvePodRole_FQDNPrefix(t *testing.T) {
	shard := &multigresv1alpha1.Shard{
		Status: multigresv1alpha1.ShardStatus{
			PodRoles: map[string]string{
				"my-pod.headless.default.svc.cluster.local": "PRIMARY",
			},
		},
	}
	role := resolvePodRole(shard, "my-pod")
	if role != "PRIMARY" {
		t.Errorf("expected PRIMARY via FQDN prefix match, got %q", role)
	}
}

func TestBuildSharedBackupPVC_InvalidStorageSize(t *testing.T) {
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
					Storage: multigresv1alpha1.StorageSpec{
						Size: "not-a-valid-quantity",
					},
				},
			},
		},
	}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_, err := BuildSharedBackupPVC(shard, "zone1", false, scheme)
	if err == nil {
		t.Fatal("expected error for invalid storage size")
	}
	if !strings.Contains(err.Error(), "invalid storage size") {
		t.Errorf("expected 'invalid storage size' error, got: %v", err)
	}
}

func TestReconcilePoolPods_ErrorPropagation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}
	poolSpec := multigresv1alpha1.PoolSpec{
		ReplicasPerCell: ptr.To(int32(1)),
		Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
	}
	poolName := "primary"
	cellName := "zone1"

	t.Run("createMissingResources error propagates", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnCreate: func(obj client.Object) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return testutil.ErrInjected
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
		err := r.reconcilePoolPods(t.Context(), shard, poolName, cellName, poolSpec)
		if err == nil {
			t.Fatal("expected error from createMissingResources")
		}
	})

	t.Run("handleScaleDown error propagates", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 0)
		labels := buildPoolLabelsWithCell(shard, poolName, cellName)

		desiredPod, _ := BuildPoolPod(shard, poolName, cellName, poolSpec, 0, scheme)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Labels:    labels,
				Annotations: map[string]string{
					metadata.AnnotationSpecHash: desiredPod.Annotations[metadata.AnnotationSpecHash],
				},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: pvcName, Namespace: "default", Labels: labels,
			},
		}

		// Extra pod at index 1 with ReadyForDeletion to trigger handleScaleDown cleanup
		extraPodName := BuildPoolPodName(shard, poolName, cellName, 1)
		extraPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      extraPodName,
				Namespace: "default",
				Labels:    labels,
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
				},
			},
		}

		base := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod, pvc, extraPod).
			Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnDelete: func(obj client.Object) error {
				if p, ok := obj.(*corev1.Pod); ok && p.Name == extraPodName {
					return testutil.ErrInjected
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
		err := r.reconcilePoolPods(t.Context(), shard, poolName, cellName, poolSpec)
		if err == nil {
			t.Fatal("expected error from handleScaleDown")
		}
	})

	t.Run("handleRollingUpdates error propagates", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		podName := BuildPoolPodName(shard, poolName, cellName, 0)
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 0)
		labels := buildPoolLabelsWithCell(shard, poolName, cellName)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        podName,
				Namespace:   "default",
				Labels:      labels,
				Annotations: map[string]string{metadata.AnnotationSpecHash: "old-hash"},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: pvcName, Namespace: "default", Labels: labels,
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, pvc).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnPatch: func(obj client.Object) error {
				if p, ok := obj.(*corev1.Pod); ok && p.Name == podName {
					return testutil.ErrInjected
				}
				return nil
			},
		})
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
		err := r.reconcilePoolPods(t.Context(), shard, poolName, cellName, poolSpec)
		if err == nil {
			t.Fatal("expected error from handleRollingUpdates")
		}
	})
}

func TestCreateMissingResources_PodBuildError(t *testing.T) {
	emptyScheme := runtime.NewScheme()

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}
	poolSpec := multigresv1alpha1.PoolSpec{
		ReplicasPerCell: ptr.To(int32(1)),
		Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
	}
	poolName := "primary"
	cellName := "zone1"

	goodScheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(goodScheme)
	_ = corev1.AddToScheme(goodScheme)

	shardCopy := shard.DeepCopy()
	pvcName := BuildPoolDataPVCName(shardCopy, poolName, cellName, 0)
	labels := buildPoolLabelsWithCell(shardCopy, poolName, cellName)
	existingPVCs := map[string]*corev1.PersistentVolumeClaim{
		pvcName: {
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "default", Labels: labels},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		},
	}

	base := fake.NewClientBuilder().
		WithScheme(goodScheme).
		WithObjects(shardCopy, existingPVCs[pvcName]).
		Build()
	r := &ShardReconciler{Client: base, Scheme: emptyScheme, Recorder: record.NewFakeRecorder(10)}

	_, _, err := r.createMissingResources(
		t.Context(), shardCopy, poolName, cellName, poolSpec,
		map[string]*corev1.Pod{}, existingPVCs, 1,
	)
	if err == nil {
		t.Fatal("expected error from BuildPoolPod with empty scheme")
	}
	if !strings.Contains(err.Error(), "failed to build pod") {
		t.Errorf("expected 'failed to build pod' error, got: %v", err)
	}
}

func TestCreateMissingResources_ExternalDeletionError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}
	poolSpec := multigresv1alpha1.PoolSpec{
		ReplicasPerCell: ptr.To(int32(1)),
		Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
	}
	poolName := "primary"
	cellName := "zone1"

	podName := BuildPoolPodName(shard, poolName, cellName, 0)
	pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 0)
	labels := buildPoolLabelsWithCell(shard, poolName, cellName)
	now := metav1.Now()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              podName,
			Namespace:         "default",
			Labels:            labels,
			DeletionTimestamp: &now,
			Finalizers:        []string{"kubernetes.io/test"},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			},
		},
	}

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
	c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
		OnPatch: func(obj client.Object) error {
			return testutil.ErrInjected
		},
	})
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	existingPods := map[string]*corev1.Pod{podName: pod}
	existingPVCs := map[string]*corev1.PersistentVolumeClaim{
		pvcName: {
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "default", Labels: labels},
		},
	}

	_, _, err := r.createMissingResources(
		t.Context(), shard, poolName, cellName, poolSpec,
		existingPods, existingPVCs, 1,
	)
	if err == nil {
		t.Fatal("expected error from handleExternalDeletion in createMissingResources")
	}
}

func TestHandleScaleDown_ExternalDeletionSetsActionTaken(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}
	poolName := "primary"
	cellName := "zone1"
	now := metav1.Now()

	extraPodName := BuildPoolPodName(shard, poolName, cellName, 1)
	extraPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              extraPodName,
			Namespace:         "default",
			Labels:            buildPoolLabelsWithCell(shard, poolName, cellName),
			DeletionTimestamp: &now,
			Finalizers:        []string{"kubernetes.io/test"},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			},
		},
	}

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, extraPod).Build()
	r := &ShardReconciler{Client: base, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	existingPods := map[string]*corev1.Pod{extraPodName: extraPod}
	actionTaken, _, err := r.handleScaleDown(
		t.Context(), shard, poolName,
		multigresv1alpha1.PoolSpec{ReplicasPerCell: ptr.To(int32(1))},
		existingPods, 1, 1, false,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !actionTaken {
		t.Error("expected actionTaken=true after external deletion of extra pod")
	}
}

func TestHandleRollingUpdates_SkipsUpToDatePods(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}
	poolName := "primary"
	cellName := "zone1"
	poolSpec := multigresv1alpha1.PoolSpec{
		ReplicasPerCell: ptr.To(int32(2)),
		Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
	}

	labels := buildPoolLabelsWithCell(shard, poolName, cellName)
	pod0Name := BuildPoolPodName(shard, poolName, cellName, 0)
	pod1Name := BuildPoolPodName(shard, poolName, cellName, 1)

	desiredPod0, _ := BuildPoolPod(shard, poolName, cellName, poolSpec, 0, scheme)
	pod0 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod0Name,
			Namespace: "default",
			Labels:    labels,
			Annotations: map[string]string{
				metadata.AnnotationSpecHash: desiredPod0.Annotations[metadata.AnnotationSpecHash],
			},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pod1Name,
			Namespace:   "default",
			Labels:      labels,
			Annotations: map[string]string{metadata.AnnotationSpecHash: "old-hash"},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}

	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard.DeepCopy(), pod0, pod1).
		Build()
	r := &ShardReconciler{Client: base, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	existingPods := map[string]*corev1.Pod{pod0Name: pod0, pod1Name: pod1}
	err := r.handleRollingUpdates(
		t.Context(), shard, poolName, cellName, poolSpec,
		existingPods, 1, false, false,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updatedPod1 corev1.Pod
	if err := base.Get(
		t.Context(),
		types.NamespacedName{Name: pod1Name, Namespace: "default"},
		&updatedPod1,
	); err != nil {
		t.Fatalf("failed to get pod1: %v", err)
	}
	if updatedPod1.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
		t.Errorf(
			"expected drifted pod1 to have drain requested, got: %q",
			updatedPod1.Annotations[metadata.AnnotationDrainState],
		)
	}
}

func TestSelectPodToDrain_AllNilEntries(t *testing.T) {
	r := &ShardReconciler{}
	shard := &multigresv1alpha1.Shard{}
	pods := []*corev1.Pod{nil, nil, nil}
	result := r.selectPodToDrain(t.Context(), pods, shard)
	if result != nil {
		t.Errorf("expected nil when all entries are nil, got %v", result)
	}
}

func TestReconcileSharedBackupPVC_BuildErrorAndNilReturn(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	t.Run("build error propagates", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-shard",
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "db",
				TableGroupName: "tg",
				ShardName:      "s1",
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
						Storage: multigresv1alpha1.StorageSpec{
							Size: "not-a-valid-quantity",
						},
					},
				},
			},
		}
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard.DeepCopy()).Build()
		r := &ShardReconciler{Client: base, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
		err := r.reconcileSharedBackupPVC(t.Context(), shard, "zone1")
		if err == nil {
			t.Fatal("expected error from build failure")
		}
		if !strings.Contains(err.Error(), "failed to build shared backup PVC") {
			t.Errorf("expected build PVC error, got: %v", err)
		}
	})
}

func TestReconcile_Deletion(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	now := metav1.Now()
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-shard-del",
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{"kubernetes.io/test"},
			Labels: map[string]string{
				metadata.LabelMultigresCluster:    "test-cluster",
				metadata.LabelMultigresDatabase:   "testdb",
				metadata.LabelMultigresTableGroup: "default",
				metadata.LabelMultigresShard:      "shard0",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "shard0",
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	reconciler := &ShardReconciler{
		Client:          c,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(100),
		APIReader:       c,
		CreateTopoStore: newMemoryTopoFactory(),
	}

	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(shard)}
	result, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", result.RequeueAfter)
	}

	var updated multigresv1alpha1.Shard
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(shard), &updated); err != nil {
		if !errors.IsNotFound(err) {
			t.Fatalf("unexpected error fetching shard: %v", err)
		}
	}
}

func TestReconcilePool_PDBError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-pdb",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}
	poolSpec := multigresv1alpha1.PoolSpec{
		Cells:           []multigresv1alpha1.CellName{"zone1"},
		ReplicasPerCell: ptr.To(int32(1)),
		Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
	}

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard.DeepCopy()).Build()
	c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
		OnPatch: func(obj client.Object) error {
			if _, ok := obj.(*policyv1.PodDisruptionBudget); ok {
				return testutil.ErrInjected
			}
			return nil
		},
	})
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
	err := r.reconcilePool(t.Context(), shard, "primary", poolSpec)
	if err == nil {
		t.Fatal("expected error from PDB reconciliation")
	}
	if !strings.Contains(err.Error(), "failed to reconcile pool PDB") {
		t.Errorf("expected PDB error, got: %v", err)
	}
}

func TestUpdateStatus_ProgressingPhase(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-progressing",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					ReplicasPerCell: ptr.To(int32(1)),
				},
			},
			MultiOrch: multigresv1alpha1.MultiOrchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
		},
		Status: multigresv1alpha1.ShardStatus{
			Phase: multigresv1alpha1.PhaseHealthy,
		},
	}

	labels := buildPoolLabelsWithCell(shard, "primary", "zone1")
	podName := BuildPoolPodName(shard, "primary", "zone1", 0)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: "default",
			Labels:    labels,
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard, pod).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	err := r.updateStatus(t.Context(), shard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shard.Status.Phase != multigresv1alpha1.PhaseProgressing {
		t.Errorf("expected PhaseProgressing, got %q", shard.Status.Phase)
	}
	if shard.Status.Message == "" {
		t.Error("expected non-empty status message for Progressing phase")
	}
}

func TestUpdatePoolsStatus_PoolEmptyEvent(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-empty",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					ReplicasPerCell: ptr.To(int32(1)),
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
	recorder := record.NewFakeRecorder(10)
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	cellsSet := make(map[multigresv1alpha1.CellName]bool)
	totalPods, readyPods, _, err := r.updatePoolsStatus(t.Context(), shard, cellsSet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if readyPods != 0 {
		t.Errorf("expected 0 ready pods, got %d", readyPods)
	}
	if totalPods != 1 {
		t.Errorf("expected 1 total pod (desired replicas), got %d", totalPods)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "PoolEmpty") {
			t.Errorf("expected PoolEmpty event, got: %s", event)
		}
	default:
		t.Error("expected PoolEmpty event to be recorded")
	}
}

func TestReconcilePool_PoolPodsError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}
	poolSpec := multigresv1alpha1.PoolSpec{
		Cells:           []multigresv1alpha1.CellName{"zone1"},
		ReplicasPerCell: ptr.To(int32(1)),
		Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
	}

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard.DeepCopy()).Build()
	c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
		OnList: func(list client.ObjectList) error {
			if _, ok := list.(*corev1.PodList); ok {
				return testutil.ErrInjected
			}
			return nil
		},
	})
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	err := r.reconcilePool(t.Context(), shard, "primary", poolSpec)
	if err == nil {
		t.Fatal("expected error from reconcilePoolPods within reconcilePool")
	}
	if !strings.Contains(err.Error(), "failed to reconcile pool pods") {
		t.Errorf("expected pool pods error, got: %v", err)
	}
}

func TestUpdateStatus_HealthyPhase(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-healthy",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					ReplicasPerCell: ptr.To(int32(1)),
				},
			},
			MultiOrch: multigresv1alpha1.MultiOrchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
		},
		Status: multigresv1alpha1.ShardStatus{
			Phase: multigresv1alpha1.PhaseProgressing,
		},
	}

	labels := buildPoolLabelsWithCell(shard, "primary", "zone1")
	podName := BuildPoolPodName(shard, "primary", "zone1", 0)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: "default",
			Labels:    labels,
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}

	// Create MultiOrch Deployment with ready replicas so OrchReady=true
	moDeployName := buildMultiOrchNameWithCell(shard, "zone1", name.DefaultConstraints)
	moSelector := map[string]string{
		metadata.LabelMultigresCluster:    "test-cluster",
		metadata.LabelMultigresDatabase:   "db",
		metadata.LabelMultigresTableGroup: "tg",
		metadata.LabelMultigresShard:      "s1",
	}
	moDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       moDeployName,
			Namespace:  "default",
			Labels:     moSelector,
			Generation: 1,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)),
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas:      1,
			ObservedGeneration: 1,
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard, pod, moDeploy).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	err := r.updateStatus(t.Context(), shard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shard.Status.Phase != multigresv1alpha1.PhaseHealthy {
		t.Errorf("expected PhaseHealthy, got %q", shard.Status.Phase)
	}
	if shard.Status.Message != "Ready" {
		t.Errorf("expected 'Ready' message, got %q", shard.Status.Message)
	}
}

func TestUpdatePoolsStatus_TerminatingPodExcluded(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-term",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					ReplicasPerCell: ptr.To(int32(1)),
				},
			},
		},
	}

	labels := buildPoolLabelsWithCell(shard, "primary", "zone1")
	now := metav1.Now()

	// Pod with deletion timestamp (terminating) - should be excluded from counts
	podName := BuildPoolPodName(shard, "primary", "zone1", 0)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              podName,
			Namespace:         "default",
			Labels:            labels,
			DeletionTimestamp: &now,
			Finalizers:        []string{"kubernetes.io/test"},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
	recorder := record.NewFakeRecorder(10)
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	cellsSet := make(map[multigresv1alpha1.CellName]bool)
	totalPods, readyPods, _, err := r.updatePoolsStatus(t.Context(), shard, cellsSet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Pod is terminating, so it should be excluded.
	// totalPods = desired replicas (1), readyPods = 0 (terminating pod excluded)
	if readyPods != 0 {
		t.Errorf("expected 0 ready pods (terminating pod excluded), got %d", readyPods)
	}
	if totalPods != 1 {
		t.Errorf("expected 1 total pod (desired replicas), got %d", totalPods)
	}
}

func TestIsDrainStale(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-stale",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"main": {
					Cells:           []multigresv1alpha1.CellName{"z1"},
					ReplicasPerCell: ptr.To(int32(5)),
				},
			},
		},
	}

	r := &ShardReconciler{Scheme: scheme}

	// Build a pod with matching spec-hash for index 4
	matchingPod := func(index int, drainState string) *corev1.Pod {
		desired, err := BuildPoolPod(shard, "main", "z1", shard.Spec.Pools["main"], index, scheme)
		if err != nil {
			t.Fatalf("BuildPoolPod failed: %v", err)
		}
		hash := ComputeSpecHash(desired)
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      BuildPoolPodName(shard, "main", "z1", index),
				Namespace: "default",
				Labels:    buildPoolLabelsWithCell(shard, "main", "z1"),
				Annotations: map[string]string{
					metadata.AnnotationSpecHash:         hash,
					metadata.AnnotationDrainState:       drainState,
					metadata.AnnotationDrainRequestedAt: "2026-03-08T18:00:00Z",
				},
			},
		}
	}

	t.Run("CancelsStaleScaleDownDrain", func(t *testing.T) {
		pod := matchingPod(4, metadata.DrainStateRequested)
		if !r.isDrainStale(shard, pod, metadata.DrainStateRequested) {
			t.Error("expected drain to be stale (pod within replicas, spec matches)")
		}
	})

	t.Run("DoesNotCancelDrainingState", func(t *testing.T) {
		pod := matchingPod(4, metadata.DrainStateDraining)
		if r.isDrainStale(shard, pod, metadata.DrainStateDraining) {
			t.Error(
				"expected drain NOT to be stale in Draining state (standby removal already sent)",
			)
		}
	})

	t.Run("DoesNotCancelExtraPodDrain", func(t *testing.T) {
		// Reduce replicas so pod-4 (index 4) is extra
		smallShard := shard.DeepCopy()
		smallShard.Spec.Pools["main"] = multigresv1alpha1.PoolSpec{
			Cells:           []multigresv1alpha1.CellName{"z1"},
			ReplicasPerCell: ptr.To(int32(4)),
		}
		pod := matchingPod(4, metadata.DrainStateRequested)
		if r.isDrainStale(smallShard, pod, metadata.DrainStateRequested) {
			t.Error("expected drain NOT to be stale (pod is extra)")
		}
	})

	t.Run("MissingLabels", func(t *testing.T) {
		pod := matchingPod(4, metadata.DrainStateRequested)
		// Clear labels to hit `if poolName == "" || cellName == ""`
		pod.Labels = nil
		if r.isDrainStale(shard, pod, metadata.DrainStateRequested) {
			t.Error("expected drain NOT to be stale (missing labels)")
		}
	})

	t.Run("MissingPoolInSpec", func(t *testing.T) {
		pod := matchingPod(4, metadata.DrainStateRequested)
		// Change pool to one that doesn't exist in spec
		pod.Labels[metadata.LabelMultigresPool] = "nonexistent"
		if r.isDrainStale(shard, pod, metadata.DrainStateRequested) {
			t.Error("expected drain NOT to be stale (pool not in spec)")
		}
	})

	t.Run("DoesNotCancelAcknowledgedDrain", func(t *testing.T) {
		pod := matchingPod(4, metadata.DrainStateAcknowledged)
		if r.isDrainStale(shard, pod, metadata.DrainStateAcknowledged) {
			t.Error("expected drain NOT to be stale (past point of no return)")
		}
	})

	t.Run("DoesNotCancelDrainedPodDrain", func(t *testing.T) {
		shardWithDrained := shard.DeepCopy()
		pod := matchingPod(0, metadata.DrainStateRequested)
		shardWithDrained.Status.PodRoles = map[string]string{
			pod.Name: "DRAINED",
		}
		if r.isDrainStale(shardWithDrained, pod, metadata.DrainStateRequested) {
			t.Error("expected drain NOT to be stale (pod role is DRAINED)")
		}
	})

	t.Run("DoesNotCancelDrainOnDeletingPod", func(t *testing.T) {
		pod := matchingPod(4, metadata.DrainStateRequested)
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		if r.isDrainStale(shard, pod, metadata.DrainStateRequested) {
			t.Error("expected drain NOT to be stale (pod is being deleted)")
		}
	})

	t.Run("DoesNotCancelWhenSpecDrifted", func(t *testing.T) {
		pod := matchingPod(4, metadata.DrainStateRequested)
		pod.Annotations[metadata.AnnotationSpecHash] = "wrong-hash"
		if r.isDrainStale(shard, pod, metadata.DrainStateRequested) {
			t.Error("expected drain NOT to be stale (spec-hash mismatch)")
		}
	})
}

func TestUpdatePoolsStatus_DrainAnnotationExcludedFromReady(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-drain",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					ReplicasPerCell: ptr.To(int32(1)),
				},
			},
			MultiOrch: multigresv1alpha1.MultiOrchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
		},
	}

	labels := buildPoolLabelsWithCell(shard, "primary", "zone1")
	podName := BuildPoolPodName(shard, "primary", "zone1", 0)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: "default",
			Labels:    labels,
			Annotations: map[string]string{
				metadata.AnnotationDrainState: "DrainStateDraining",
			},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
	recorder := record.NewFakeRecorder(10)
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	cellsSet := make(map[multigresv1alpha1.CellName]bool)
	totalPods, readyPods, _, err := r.updatePoolsStatus(t.Context(), shard, cellsSet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if readyPods != 0 {
		t.Errorf("expected 0 ready pods (draining pod excluded), got %d", readyPods)
	}
	if totalPods != 1 {
		t.Errorf("expected 1 total pod (desired replicas), got %d", totalPods)
	}
}

func TestUpdatePoolsStatus_DegradedOnCrashLoop(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-crash",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					ReplicasPerCell: ptr.To(int32(1)),
				},
			},
			MultiOrch: multigresv1alpha1.MultiOrchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
		},
	}

	labels := buildPoolLabelsWithCell(shard, "primary", "zone1")
	podName := BuildPoolPodName(shard, "primary", "zone1", 0)

	t.Run("CrashLoopBackOff", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Labels:    labels,
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "postgres",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "CrashLoopBackOff",
							},
						},
					},
				},
			},
		}

		moDeployName := buildMultiOrchNameWithCell(shard, "zone1", name.DefaultConstraints)
		moDeploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:       moDeployName,
				Namespace:  "default",
				Generation: 1,
			},
			Spec:   appsv1.DeploymentSpec{Replicas: ptr.To(int32(1))},
			Status: appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
		}

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod, moDeploy).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.updateStatus(t.Context(), shard); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if shard.Status.Phase != multigresv1alpha1.PhaseDegraded {
			t.Errorf("expected PhaseDegraded for CrashLoopBackOff pod, got %q", shard.Status.Phase)
		}
	})

	t.Run("OOMKilled", func(t *testing.T) {
		s := shard.DeepCopy()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName + "-oom",
				Namespace: "default",
				Labels:    labels,
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "postgres",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "OOMKilled",
							},
						},
					},
				},
			},
		}

		moDeployName := buildMultiOrchNameWithCell(shard, "zone1", name.DefaultConstraints)
		moDeploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:       moDeployName,
				Namespace:  "default",
				Generation: 1,
			},
			Spec:   appsv1.DeploymentSpec{Replicas: ptr.To(int32(1))},
			Status: appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
		}

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(s, pod, moDeploy).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.updateStatus(t.Context(), s); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.Status.Phase != multigresv1alpha1.PhaseDegraded {
			t.Errorf("expected PhaseDegraded for OOMKilled pod, got %q", s.Status.Phase)
		}
	})

	t.Run("RunningPodNotDegraded", func(t *testing.T) {
		s := shard.DeepCopy()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName + "-running",
				Namespace: "default",
				Labels:    labels,
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "postgres",
						// RestartCount is non-zero but pod is currently Running — not degraded
						RestartCount: 3,
						State: corev1.ContainerState{
							Running: &corev1.ContainerStateRunning{},
						},
					},
				},
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				},
			},
		}

		moDeployName := buildMultiOrchNameWithCell(shard, "zone1", name.DefaultConstraints)
		moDeploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:       moDeployName,
				Namespace:  "default",
				Generation: 1,
			},
			Spec:   appsv1.DeploymentSpec{Replicas: ptr.To(int32(1))},
			Status: appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
		}

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(s, pod, moDeploy).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		if err := r.updateStatus(t.Context(), s); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.Status.Phase == multigresv1alpha1.PhaseDegraded {
			t.Errorf(
				"expected Progressing (not Degraded) for running pod with prior restarts, got %q",
				s.Status.Phase,
			)
		}
	})
}

func TestUpdateStatus_DegradedOnMultiOrchCrashLoop(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-orch-crash",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					ReplicasPerCell: ptr.To(int32(1)),
				},
			},
			MultiOrch: multigresv1alpha1.MultiOrchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
		},
	}

	// Pool pod is healthy
	poolLabels := buildPoolLabelsWithCell(shard, "primary", "zone1")
	poolPodName := BuildPoolPodName(shard, "primary", "zone1", 0)
	poolPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolPodName,
			Namespace: "default",
			Labels:    poolLabels,
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
			},
		},
	}

	// MultiOrch deployment exists but pod is crash-looping
	moDeployName := buildMultiOrchNameWithCell(shard, "zone1", name.DefaultConstraints)
	moDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       moDeployName,
			Namespace:  "default",
			Generation: 1,
		},
		Spec:   appsv1.DeploymentSpec{Replicas: ptr.To(int32(1))},
		Status: appsv1.DeploymentStatus{Replicas: 1, ReadyReplicas: 0, ObservedGeneration: 1},
	}

	moLabels := buildMultiOrchLabelsWithCell(shard, "zone1")
	moPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      moDeployName + "-abc",
			Namespace: "default",
			Labels:    metadata.GetSelectorLabels(moLabels),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "multiorch",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard, poolPod, moDeploy, moPod).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := r.updateStatus(t.Context(), shard); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shard.Status.Phase != multigresv1alpha1.PhaseDegraded {
		t.Errorf("expected PhaseDegraded for crash-looping MultiOrch, got %q", shard.Status.Phase)
	}
	if shard.Status.Message != "One or more MultiOrch pods are crash-looping" {
		t.Errorf("expected MultiOrch-specific degraded message, got %q", shard.Status.Message)
	}
}

func TestExpandPVCIfNeeded(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
	}

	t.Run("no-op when sizes are equal", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data-pvc-0", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard.DeepCopy(), pvc).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		poolSpec := multigresv1alpha1.PoolSpec{
			Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
		}
		if err := r.expandPVCIfNeeded(t.Context(), shard, pvc, poolSpec); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := &corev1.PersistentVolumeClaim{}
		_ = c.Get(t.Context(), types.NamespacedName{Name: "data-pvc-0", Namespace: "default"}, got)
		current := got.Spec.Resources.Requests[corev1.ResourceStorage]
		if current.Cmp(resource.MustParse("10Gi")) != 0 {
			t.Errorf("expected 10Gi, got %s", current.String())
		}
	})

	t.Run("patches PVC when desired is larger", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data-pvc-1", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard.DeepCopy(), pvc).Build()
		recorder := record.NewFakeRecorder(10)
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: recorder}

		poolSpec := multigresv1alpha1.PoolSpec{
			Storage: multigresv1alpha1.StorageSpec{Size: "20Gi"},
		}
		if err := r.expandPVCIfNeeded(t.Context(), shard, pvc, poolSpec); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := &corev1.PersistentVolumeClaim{}
		_ = c.Get(t.Context(), types.NamespacedName{Name: "data-pvc-1", Namespace: "default"}, got)
		current := got.Spec.Resources.Requests[corev1.ResourceStorage]
		if current.Cmp(resource.MustParse("20Gi")) != 0 {
			t.Errorf("expected 20Gi after expansion, got %s", current.String())
		}
	})

	t.Run("no-op when desired is smaller", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data-pvc-2", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("20Gi"),
					},
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard.DeepCopy(), pvc).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		poolSpec := multigresv1alpha1.PoolSpec{
			Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
		}
		if err := r.expandPVCIfNeeded(t.Context(), shard, pvc, poolSpec); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := &corev1.PersistentVolumeClaim{}
		_ = c.Get(t.Context(), types.NamespacedName{Name: "data-pvc-2", Namespace: "default"}, got)
		current := got.Spec.Resources.Requests[corev1.ResourceStorage]
		if current.Cmp(resource.MustParse("20Gi")) != 0 {
			t.Errorf("expected 20Gi unchanged, got %s", current.String())
		}
	})

	t.Run("handles nil requests map", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data-pvc-3", Namespace: "default"},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard.DeepCopy(), pvc).Build()
		recorder := record.NewFakeRecorder(10)
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: recorder}

		poolSpec := multigresv1alpha1.PoolSpec{
			Storage: multigresv1alpha1.StorageSpec{Size: "5Gi"},
		}
		if err := r.expandPVCIfNeeded(t.Context(), shard, pvc, poolSpec); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := &corev1.PersistentVolumeClaim{}
		_ = c.Get(t.Context(), types.NamespacedName{Name: "data-pvc-3", Namespace: "default"}, got)
		current := got.Spec.Resources.Requests[corev1.ResourceStorage]
		if current.Cmp(resource.MustParse("5Gi")) != 0 {
			t.Errorf("expected 5Gi, got %s", current.String())
		}
	})
}

func TestPVCNeedsFilesystemResize(t *testing.T) {
	t.Parallel()

	t.Run("returns true when condition present", func(t *testing.T) {
		pvcs := map[string]*corev1.PersistentVolumeClaim{
			"data-pvc-0": {
				Status: corev1.PersistentVolumeClaimStatus{
					Conditions: []corev1.PersistentVolumeClaimCondition{
						{
							Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
		}
		if !pvcNeedsFilesystemResize(pvcs, "data-pvc-0") {
			t.Error("expected true for FileSystemResizePending condition")
		}
	})

	t.Run("returns false when no condition", func(t *testing.T) {
		pvcs := map[string]*corev1.PersistentVolumeClaim{
			"data-pvc-0": {},
		}
		if pvcNeedsFilesystemResize(pvcs, "data-pvc-0") {
			t.Error("expected false when no conditions")
		}
	})

	t.Run("returns false for unknown PVC", func(t *testing.T) {
		pvcs := map[string]*corev1.PersistentVolumeClaim{}
		if pvcNeedsFilesystemResize(pvcs, "missing") {
			t.Error("expected false for unknown PVC name")
		}
	})
}

func TestResolvePodRole(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		podRoles map[string]string
		podName  string
		want     string
	}{
		"nil roles": {
			podRoles: nil,
			podName:  "pod-0",
			want:     "",
		},
		"exact match": {
			podRoles: map[string]string{"pod-0": "PRIMARY"},
			podName:  "pod-0",
			want:     "PRIMARY",
		},
		"FQDN prefix match": {
			podRoles: map[string]string{"pod-0.svc.cluster.local": "REPLICA"},
			podName:  "pod-0",
			want:     "REPLICA",
		},
		"no match": {
			podRoles: map[string]string{"pod-1": "PRIMARY"},
			podName:  "pod-0",
			want:     "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			shard := &multigresv1alpha1.Shard{
				Status: multigresv1alpha1.ShardStatus{
					PodRoles: tc.podRoles,
				},
			}
			if got := resolvePodRole(shard, tc.podName); got != tc.want {
				t.Errorf("resolvePodRole() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsPoolerPruningEnabled(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		shard *multigresv1alpha1.Shard
		want  bool
	}{
		"nil TopologyPruning defaults to true": {
			shard: &multigresv1alpha1.Shard{},
			want:  true,
		},
		"nil Enabled defaults to true": {
			shard: &multigresv1alpha1.Shard{
				Spec: multigresv1alpha1.ShardSpec{
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{},
				},
			},
			want: true,
		},
		"explicitly enabled": {
			shard: &multigresv1alpha1.Shard{
				Spec: multigresv1alpha1.ShardSpec{
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
						Enabled: ptr.To(true),
					},
				},
			},
			want: true,
		},
		"explicitly disabled": {
			shard: &multigresv1alpha1.Shard{
				Spec: multigresv1alpha1.ShardSpec{
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
						Enabled: ptr.To(false),
					},
				},
			},
			want: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := isPoolerPruningEnabled(tc.shard); got != tc.want {
				t.Errorf("isPoolerPruningEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEnqueueFromPostgresConfigMap(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardWithRef := &multigresv1alpha1.Shard{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "multigres.com/v1alpha1",
			Kind:       "Shard",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shard-with-ref",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.ShardSpec{
			PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
				Name: "my-pg-config",
				Key:  "postgresql.conf",
			},
		},
	}

	shardNoRef := &multigresv1alpha1.Shard{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "multigres.com/v1alpha1",
			Kind:       "Shard",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shard-no-ref",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.ShardSpec{},
	}

	shardDifferentRef := &multigresv1alpha1.Shard{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "multigres.com/v1alpha1",
			Kind:       "Shard",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shard-different-ref",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.ShardSpec{
			PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
				Name: "other-config",
				Key:  "postgresql.conf",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardWithRef, shardNoRef, shardDifferentRef).
		Build()

	reconciler := &ShardReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pg-config",
			Namespace: "default",
		},
	}

	requests := reconciler.enqueueFromPostgresConfigMap(context.Background(), cm)

	if len(requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(requests))
	}
	if requests[0].Name != "shard-with-ref" {
		t.Errorf("enqueued shard = %q, want %q", requests[0].Name, "shard-with-ref")
	}
}

func TestComputePostgresConfigHash(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	t.Run("returns hash of referenced key", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "pg-config", Namespace: "default"},
			Data:       map[string]string{"custom.conf": "shared_buffers = '8GB'"},
		}
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
					Name: "pg-config",
					Key:  "custom.conf",
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		r := &ShardReconciler{Client: c}

		hash, err := r.computePostgresConfigHash(context.Background(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hash == "" {
			t.Error("hash should not be empty")
		}
		if len(hash) != 64 {
			t.Errorf("hash length = %d, want 64 (SHA-256 hex)", len(hash))
		}

		// Same content should produce same hash
		hash2, _ := r.computePostgresConfigHash(context.Background(), shard)
		if hash != hash2 {
			t.Errorf("hash not deterministic: %q != %q", hash, hash2)
		}
	})

	t.Run("different content produces different hash", func(t *testing.T) {
		cm1 := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "pg-v1", Namespace: "default"},
			Data:       map[string]string{"pg.conf": "shared_buffers = '4GB'"},
		}
		cm2 := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "pg-v2", Namespace: "default"},
			Data:       map[string]string{"pg.conf": "shared_buffers = '8GB'"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm1, cm2).Build()
		r := &ShardReconciler{Client: c}

		shard1 := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
					Name: "pg-v1",
					Key:  "pg.conf",
				},
			},
		}
		shard2 := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "s2", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
					Name: "pg-v2",
					Key:  "pg.conf",
				},
			},
		}

		h1, _ := r.computePostgresConfigHash(context.Background(), shard1)
		h2, _ := r.computePostgresConfigHash(context.Background(), shard2)
		if h1 == h2 {
			t.Error("different ConfigMap content should produce different hashes")
		}
	})

	t.Run("missing key returns error", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "pg-config", Namespace: "default"},
			Data:       map[string]string{"other.conf": "value"},
		}
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
					Name: "pg-config",
					Key:  "missing-key",
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		r := &ShardReconciler{Client: c}

		_, err := r.computePostgresConfigHash(context.Background(), shard)
		if err == nil {
			t.Error("expected error for missing key")
		}
		if !strings.Contains(err.Error(), "missing-key") {
			t.Errorf("error should mention missing key, got: %v", err)
		}
	})

	t.Run("missing ConfigMap returns error", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
					Name: "nonexistent",
					Key:  "pg.conf",
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ShardReconciler{Client: c}

		_, err := r.computePostgresConfigHash(context.Background(), shard)
		if err == nil {
			t.Error("expected error for missing ConfigMap")
		}
	})
}

// TestReconcilePoolPods_AdditionalErrorPaths covers the error return paths of reconcilePoolPods and its helpers
func TestReconcilePoolPods_AdditionalErrorPaths(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"main": {
					ReplicasPerCell: ptr.To(int32(1)),
				},
			},
		},
	}
	poolSpec := shard.Spec.Pools["main"]

	t.Run("syncDrainedLabels error", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      BuildPoolPodName(shard, "main", "z1", 0),
				Namespace: "default",
				Labels: map[string]string{
					metadata.LabelMultigresCluster:    "test-cluster",
					metadata.LabelMultigresDatabase:   "db",
					metadata.LabelMultigresTableGroup: "tg",
					metadata.LabelMultigresShard:      "test-shard",
					metadata.LabelMultigresPool:       "main",
					metadata.LabelMultigresCell:       "z1",
					metadata.LabelPodRole:             "DRAINED",
				},
			},
		}

		baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
		fails := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnPatch: testutil.FailOnObjectName(pod.Name, testutil.ErrPermissionError),
		})

		r := &ShardReconciler{Client: fails, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		// Temporarily set the topology returned role to NOT DRAINED, which causes syncDrainedLabels to try and strip the label
		// which triggers the patch failure
		// Wait, the test uses resolvePodRole, which defaults to whatever unless mocked.
		// Since topo is not mocked here, resolvePodRole returns "" -> not DRAINED -> tries to patch remove DRAINED -> fails
		existingPods := map[string]*corev1.Pod{
			pod.Name: pod,
		}
		err := r.syncDrainedLabels(context.Background(), shard, existingPods)
		if err == nil || !strings.Contains(err.Error(), "failed to remove DRAINED label") {
			t.Fatalf("expected syncDrainedLabels error, got %v", err)
		}
	})

	t.Run("CreatePVC error", func(t *testing.T) {
		baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		pvcName := BuildPoolDataPVCName(shard, "main", "z1", 0)
		fails := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnCreate: testutil.FailOnObjectName(pvcName, testutil.ErrPermissionError),
		})

		r := &ShardReconciler{Client: fails, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
		err := r.reconcilePoolPods(context.Background(), shard, "main", "z1", poolSpec)
		if err == nil || !strings.Contains(err.Error(), "failed to create PVC") {
			t.Fatalf("expected PVC creation error, got %v", err)
		}
	})

	t.Run("deletePodPVC network error", func(t *testing.T) {
		// Mock a DRAINED pod so deletePodPVC is called during cleanupDrainedPod
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      BuildPoolPodName(shard, "main", "z1", 0),
				Namespace: "default",
				Labels: map[string]string{
					metadata.LabelMultigresCluster:    "test-cluster",
					metadata.LabelMultigresDatabase:   "db",
					metadata.LabelMultigresTableGroup: "tg",
					metadata.LabelMultigresShard:      "test-shard",
					metadata.LabelMultigresPool:       "main",
					metadata.LabelMultigresCell:       "z1",
					metadata.LabelPodRole:             "DRAINED",
				},
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
				},
			},
		}

		pvcName := BuildPoolDataPVCName(shard, "main", "z1", 0)
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: "default",
			},
		}

		baseClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod, pvc).
			Build()
		fails := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnDelete: testutil.FailOnObjectName(pvcName, testutil.ErrPermissionError),
		})

		r := &ShardReconciler{Client: fails, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
		err := r.cleanupDrainedPod(context.Background(), shard, pod, "main", poolSpec, 1)
		if err == nil || !strings.Contains(err.Error(), "failed to delete PVC") {
			t.Fatalf("expected PVC deletion error, got %v", err)
		}
	})
}
