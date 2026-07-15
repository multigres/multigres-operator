package shard

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

type reconcileTestCase struct {
	shard            *multigresv1alpha1.Shard
	existingObjects  []client.Object
	failureConfig    *testutil.FailureConfig
	reconcilerScheme *runtime.Scheme
	wantErr          bool
	assertFunc       func(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard)
}

func TestShardReconciler_Reconcile(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	tests := map[string]reconcileTestCase{
		////----------------------------------------
		///   Success
		//------------------------------------------
		"create all resources for new Shard with single pool": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{"zone1"},
							Type:            "replica",
							ReplicasPerCell: ptr.To(int32(1)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			assertFunc: func(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard) {
				// Verify Multiorch Deployment was created (with cell suffix)
				moDeploy := &appsv1.Deployment{}
				hashedMoName := buildHashedMultiorchName(shard, "zone1")
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedMoName, Namespace: "default"},
					moDeploy); err != nil {
					t.Errorf("Multiorch Deployment should exist: %v", err)
				}

				// Verify Multiorch Service was created (with cell suffix)
				moSvc := &corev1.Service{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedMoName, Namespace: "default"},
					moSvc); err != nil {
					t.Errorf("Multiorch Service should exist: %v", err)
				}

				// Verify Pool Pod and PVC were created
				podName := BuildPoolPodName(shard, "primary", "zone1", 0)
				pod := &corev1.Pod{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: podName, Namespace: "default"},
					pod); err != nil {
					t.Errorf("Pool Pod should exist: %v", err)
				}

				pvcName := BuildPoolDataPVCName(shard, "primary", "zone1", 0)
				pvc := &corev1.PersistentVolumeClaim{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: pvcName, Namespace: "default"},
					pvc); err != nil {
					t.Errorf("Pool PVC should exist: %v", err)
				}

				// Verify Pool headless Service was created (with cell suffix)
				hashedHeadless := buildHashedPoolHeadlessServiceName(shard, "primary", "zone1")
				poolSvc := &corev1.Service{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedHeadless, Namespace: "default"},
					poolSvc); err != nil {
					t.Errorf("Pool headless Service should exist: %v", err)
				}
			},
		},
		"create resources for Shard with multiple pools": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multi-pool-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"replica": {
							Cells:           []multigresv1alpha1.CellName{"zone1"},
							Type:            "replica",
							ReplicasPerCell: ptr.To(int32(2)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
						"read-pool": {
							Cells:           []multigresv1alpha1.CellName{"zone1"},
							Type:            "readWrite",
							ReplicasPerCell: ptr.To(int32(3)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "5Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			assertFunc: func(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard) {
				// Verify replica pool pods
				for i := 0; i < 2; i++ {
					podName := BuildPoolPodName(shard, "replica", "zone1", i)
					if err := c.Get(
						t.Context(),
						types.NamespacedName{Name: podName, Namespace: "default"},
						&corev1.Pod{},
					); err != nil {
						t.Errorf("Replica pool Pod %d should exist: %v", i, err)
					}
				}

				// Verify read-pool pods
				for i := 0; i < 3; i++ {
					podName := BuildPoolPodName(shard, "read-pool", "zone1", i)
					if err := c.Get(
						t.Context(),
						types.NamespacedName{Name: podName, Namespace: "default"},
						&corev1.Pod{},
					); err != nil {
						t.Errorf("read-pool Pod %d should exist: %v", i, err)
					}
				}

				// Verify both headless services
				hashReplicaHeadless := buildHashedPoolHeadlessServiceName(shard, "replica", "zone1")
				replicaSvc := &corev1.Service{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashReplicaHeadless, Namespace: "default"},
					replicaSvc); err != nil {
					t.Errorf("Replica pool headless Service should exist: %v", err)
				}

				hashReadPoolHeadless := buildHashedPoolHeadlessServiceName(
					shard,
					"read-pool",
					"zone1",
				)
				readPoolSvc := &corev1.Service{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashReadPoolHeadless, Namespace: "default"},
					readPoolSvc); err != nil {
					t.Errorf("read-pool headless Service should exist: %v", err)
				}
			},
		},
		"Multiorch infers cells from pools when not specified": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "inferred-cells-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{}, // Empty - will infer from pools
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{"zone1", "zone2"},
							Type:            "replica",
							ReplicasPerCell: ptr.To(int32(1)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			assertFunc: func(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard) {
				// Multiorch should be deployed to both zone1 and zone2
				hashedMo1 := buildHashedMultiorchName(shard, "zone1")
				mo1 := &appsv1.Deployment{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedMo1, Namespace: "default"},
					mo1); err != nil {
					t.Errorf("Multiorch Deployment for zone1 should exist: %v", err)
				}

				hashedMo2 := buildHashedMultiorchName(shard, "zone2")
				mo2 := &appsv1.Deployment{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedMo2, Namespace: "default"},
					mo2); err != nil {
					t.Errorf("Multiorch Deployment for zone2 should exist: %v", err)
				}
			},
		},
		"create backup PVCs for all active cells including pool-only cells": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pool-only-pvc-shard",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Backup: &multigresv1alpha1.BackupConfig{
						Type:       multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{},
					},
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{"zone1"},
							Type:            "replica",
							ReplicasPerCell: ptr.To(int32(1)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
						"readonly": {
							Cells:           []multigresv1alpha1.CellName{"zone2"},
							Type:            "readWrite",
							ReplicasPerCell: ptr.To(int32(1)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			assertFunc: func(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard) {
				// Verify Multiorch Deployment was ONLY created for zone1
				hashedMo1 := buildHashedMultiorchName(shard, "zone1")
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedMo1, Namespace: "default"},
					&appsv1.Deployment{}); err != nil {
					t.Errorf("Multiorch Deployment for zone1 should exist: %v", err)
				}
				hashedMo2 := buildHashedMultiorchName(shard, "zone2")
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedMo2, Namespace: "default"},
					&appsv1.Deployment{}); err == nil {
					t.Errorf("Multiorch Deployment for zone2 should NOT exist")
				}

				// Verify Backup PVCs were created for BOTH zone1 AND zone2
				hashedPvc1 := buildHashedBackupPVCName(shard, "zone1")
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedPvc1, Namespace: "default"},
					&corev1.PersistentVolumeClaim{}); err != nil {
					t.Errorf("Backup PVC for zone1 should exist: %v", err)
				}
				hashedPvc2 := buildHashedBackupPVCName(shard, "zone2")
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedPvc2, Namespace: "default"},
					&corev1.PersistentVolumeClaim{}); err != nil {
					t.Errorf("Backup PVC for zone2 should exist: %v", err)
				}
			},
		},
		"error when Multiorch and pools have no cells specified": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-cells-anywhere",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{}, // Empty
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{}, // Also empty - should error
							Type:            "replica",
							ReplicasPerCell: ptr.To(int32(1)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			wantErr:         true,
		},
		"error when pool has no cells specified": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-cell-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{}, // Empty cells - should error
							Type:            "replica",
							ReplicasPerCell: ptr.To(int32(1)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			wantErr:         true,
		},
		"create resources for Shard with multi-cell pool": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multi-cell-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1", "zone2"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{"zone1", "zone2"},
							Type:            "replica",
							ReplicasPerCell: ptr.To(int32(2)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			assertFunc: func(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard) {
				// Verify Pods for zone1
				for i := 0; i < 2; i++ {
					podName := BuildPoolPodName(shard, "primary", "zone1", i)
					if err := c.Get(
						t.Context(),
						types.NamespacedName{Name: podName, Namespace: "default"},
						&corev1.Pod{},
					); err != nil {
						t.Errorf("Zone1 Pod %d should exist: %v", i, err)
					}
				}

				// Verify Pods for zone2
				for i := 0; i < 2; i++ {
					podName := BuildPoolPodName(shard, "primary", "zone2", i)
					if err := c.Get(
						t.Context(),
						types.NamespacedName{Name: podName, Namespace: "default"},
						&corev1.Pod{},
					); err != nil {
						t.Errorf("Zone2 Pod %d should exist: %v", i, err)
					}
				}

				// Verify headless Services for both cells
				hashSvc1 := buildHashedPoolHeadlessServiceName(shard, "primary", "zone1")
				svc1 := &corev1.Service{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashSvc1, Namespace: "default"},
					svc1); err != nil {
					t.Fatalf("Headless Service for zone1 should exist: %v", err)
				}

				hashSvc2 := buildHashedPoolHeadlessServiceName(shard, "primary", "zone2")
				svc2 := &corev1.Service{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashSvc2, Namespace: "default"},
					svc2); err != nil {
					t.Fatalf("Headless Service for zone2 should exist: %v", err)
				}
			},
		},
		"update existing resources": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Images: multigresv1alpha1.ShardImages{
						Multipooler: "custom/multipooler:v1.0.0",
						Postgres:    "postgres:16",
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{"zone1"},
							Type:            "replica",
							ReplicasPerCell: ptr.To(int32(5)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "20Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "existing-shard-multiorch",
						Namespace: "default",
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(2)),
					},
					Status: appsv1.DeploymentStatus{
						Replicas:      2,
						ReadyReplicas: 2,
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "existing-shard-multiorch",
						Namespace: "default",
					},
				},
			},
			assertFunc: func(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard) {
				// Verify scale up to 5 pods
				for i := 0; i < 5; i++ {
					podName := BuildPoolPodName(shard, "primary", "zone1", i)
					if err := c.Get(
						t.Context(),
						types.NamespacedName{Name: podName, Namespace: "default"},
						&corev1.Pod{},
					); err != nil {
						t.Errorf("Zone1 Pod %d should exist: %v", i, err)
					}
				}
			},
		},

		"deletion - early exit": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-shard-deletion",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					Finalizers:        []string{"kubernetes.io/test"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{"zone1"},
							Type:            "replica",
							ReplicasPerCell: ptr.To(int32(1)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "test-shard-deletion",
						Namespace:         "default",
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
						Finalizers:        []string{"testing"},
					},
					Spec: multigresv1alpha1.ShardSpec{
						DatabaseName:   "testdb",
						TableGroupName: "default",
						Multiorch: multigresv1alpha1.MultiorchSpec{
							Cells: []multigresv1alpha1.CellName{"zone1"},
						},
						Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
							"primary": {
								Cells:           []multigresv1alpha1.CellName{"zone1"},
								Type:            "replica",
								ReplicasPerCell: ptr.To(int32(1)),
								Storage: multigresv1alpha1.StorageSpec{
									Size: "10Gi",
								},
							},
						},
					},
				},
			},
			assertFunc: func(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard) {
				// Verify Multiorch Deployment was NOT created
				moDeploy := &appsv1.Deployment{}
				hashedMoName := buildHashedMultiorchName(shard, "zone1")
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedMoName, Namespace: "default"},
					moDeploy); err == nil {
					t.Errorf("Multiorch Deployment should NOT exist")
				}
			},
		},

		////----------------------------------------
		///   Error
		//------------------------------------------
		"error on status update": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells: []multigresv1alpha1.CellName{"zone1"},
							Type:  "replica",
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnStatusPatch: testutil.FailOnObjectName("test-shard", testutil.ErrInjected),
			},
			wantErr: true,
		},
		"error on Multiorch Deployment patch": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells: []multigresv1alpha1.CellName{"zone1"},
							Type:  "replica",
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if deploy, ok := obj.(*appsv1.Deployment); ok &&
						strings.Contains(
							deploy.Name,
							"multiorch",
						) && strings.Contains(deploy.Name, "zone1") {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},

		"error on Multiorch Service patch": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells: []multigresv1alpha1.CellName{"zone1"},
							Type:  "replica",
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if svc, ok := obj.(*corev1.Service); ok &&
						strings.Contains(
							svc.Name,
							"multiorch",
						) && strings.Contains(svc.Name, "zone1") {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},

		"error on Pool PDB patch": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells: []multigresv1alpha1.CellName{"zone1"},
							Type:  "replica",
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if pdb, ok := obj.(*policyv1.PodDisruptionBudget); ok &&
						strings.Contains(
							pdb.Name,
							"pdb",
						) {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},

		"error on Pool Service patch": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells: []multigresv1alpha1.CellName{"zone1"},
							Type:  "replica",
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if svc, ok := obj.(*corev1.Service); ok &&
						strings.Contains(
							svc.Name,
							"pool",
						) && strings.Contains(svc.Name, "primary") &&
						strings.Contains(
							svc.Name,
							"zone1",
						) && strings.Contains(svc.Name, "headless") {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},

		"error on Get Shard (network error)": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells: []multigresv1alpha1.CellName{"zone1"},
							Type:  "replica",
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnKeyName("test-shard", testutil.ErrNetworkTimeout),
			},
			wantErr: true,
		},
		"error on pg_hba ConfigMap patch": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells: []multigresv1alpha1.CellName{"zone1"},
							Type:  "replica",
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if cm, ok := obj.(*corev1.ConfigMap); ok &&
						strings.Contains(cm.Name, "pg-hba") {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},
		"error on Pool Backup PVC patch": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Backup: &multigresv1alpha1.BackupConfig{
						Type:       multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{},
					},
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells: []multigresv1alpha1.CellName{"zone1"},
							Type:  "replica",
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok &&
						strings.Contains(
							pvc.Name,
							"backup-data-test-cluster-testdb-default--zone1",
						) {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},
		"error on Get Pool StatefulSet in updateStatus (network error)": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-shard-status",
					Namespace:  "default",
					Finalizers: []string{"shard.multigres.com/finalizer"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells: []multigresv1alpha1.CellName{"zone1"},
							Type:  "replica",
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
				},
			},
			existingObjects: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-status-multiorch-zone1",
						Namespace: "default",
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-status-multiorch-zone1",
						Namespace: "default",
					},
				},
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-status-pool-primary-zone1",
						Namespace: "default",
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-status-pool-primary-zone1-headless",
						Namespace: "default",
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				// Fail Pool StatefulSet Get in updateStatus
				// With SSA, the only Get calls are:
				// 1. Shard (at start of Reconcile)
				// 2. Pool StatefulSet (in updateStatus)
				// So we want to fail the 2nd Get call.
				OnGet: testutil.FailKeyAfterNCalls(1, testutil.ErrNetworkTimeout),
			},
			wantErr: true,
		},
		"error on build PgHba ConfigMap (empty scheme)": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "build-err-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
				},
			},
			reconcilerScheme: runtime.NewScheme(), // Empty scheme
			wantErr:          true,
		},
		"error on Get PgHba ConfigMap (network error)": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard-pghba-get-err",
					Namespace: "default",
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnGet: func(key client.ObjectKey) error {
					if key.Name == PgHbaConfigMapName("test-shard-pghba-get-err") {
						return testutil.ErrNetworkTimeout
					}
					return nil
				},
			},
			wantErr: true,
		},
		"error on build Multiorch Deployment (scheme missing Shard)": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "build-mo-err-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
				},
			},
			reconcilerScheme: runtime.NewScheme(),
			wantErr:          true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			setTestPostgresPasswordSecretRef(tc.shard)
			tc.existingObjects = append(
				tc.existingObjects,
				testPostgresPasswordSecretForShard(tc.shard),
			)

			// Patch existing objects names to use hashed names
			for i, obj := range tc.existingObjects {
				name := obj.GetName()
				replaced := false

				// Check Multiorch
				for _, cell := range tc.shard.Spec.Multiorch.Cells {
					if strings.Contains(name, "multiorch") && strings.Contains(name, string(cell)) {
						hashed := buildHashedMultiorchName(tc.shard, string(cell))
						obj.SetName(hashed)
						// Update labels/selectors if applicable
						if deploy, ok := obj.(*appsv1.Deployment); ok {
							if deploy.Labels != nil {
								deploy.Labels["app.kubernetes.io/instance"] = hashed
							}
							if deploy.Spec.Selector != nil {
								deploy.Spec.Selector.MatchLabels["app.kubernetes.io/instance"] = hashed
							}
						}
						// Service selector update? Service selector uses labels.
						// We don't update Service contents here usually, just name.
						replaced = true
						break
					}
				}
				if replaced {
					tc.existingObjects[i] = obj
					continue
				}

				// Check Pools
				for poolName, poolSpec := range tc.shard.Spec.Pools {
					for _, cell := range poolSpec.Cells {
						if strings.Contains(name, "pool") &&
							strings.Contains(name, string(poolName)) &&
							strings.Contains(name, string(cell)) {
							// Determine if headless svc or backup pvc
							if strings.Contains(name, "headless") {
								hashed := buildHashedPoolHeadlessServiceName(
									tc.shard,
									string(poolName),
									string(cell),
								)
								obj.SetName(hashed)
							} else if strings.Contains(name, "backup-data") {
								hashed := buildHashedBackupPVCName(
									tc.shard,
									string(cell),
								)
								obj.SetName(hashed)
							} else {
								// StatefulSet or Service (if not headless - wait, pool service IS headless)
								// Wait, is there a non-headless service for pool?
								// pool_service.go creates headless. Use buildHashedPoolHeadlessServiceName.
								// Check if obj kind is Service.
								// But "multiorch" handled above. "pool" here.
								// Pool creates StatefulSet and Headless Service.

								// What if test creates a generic service?
								// The tests create: "test-shard-pool-primary-zone1-headless".

								// What about "pool-primary-zone1" (StatefulSet)?
								hashed := buildHashedPoolName(
									tc.shard,
									string(poolName),
									string(cell),
								)
								if !strings.Contains(name, "headless") &&
									!strings.Contains(name, "backup-data") {
									obj.SetName(hashed)
									if sts, ok := obj.(*appsv1.StatefulSet); ok {
										if sts.Labels != nil {
											sts.Labels["app.kubernetes.io/instance"] = hashed
										}
										if sts.Spec.Selector != nil {
											sts.Spec.Selector.MatchLabels["app.kubernetes.io/instance"] = hashed
										}
										// ServiceName in STS must match Headless Service Name!
										// We need to update sts.Spec.ServiceName to hashed headless name.
										headlessName := buildHashedPoolHeadlessServiceName(
											tc.shard,
											string(poolName),
											string(cell),
										)
										sts.Spec.ServiceName = headlessName
									}
								}
							}
							replaced = true
							break
						}
					}
					if replaced {
						break
					}
				}
				tc.existingObjects[i] = obj
			}

			// Create base fake client
			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.existingObjects...).
				WithStatusSubresource(&multigresv1alpha1.Shard{}).
				WithStatusSubresource(&appsv1.StatefulSet{}).
				WithStatusSubresource(&appsv1.Deployment{}).
				WithStatusSubresource(&corev1.Pod{}).
				Build()

			fakeClient := client.Client(baseClient)

			// Wrap with failure injection if configured
			if tc.failureConfig != nil {
				fakeClient = testutil.NewFakeClientWithFailures(baseClient, tc.failureConfig)
			}

			reconciler := &ShardReconciler{
				Client:          fakeClient,
				Scheme:          scheme,
				Recorder:        record.NewFakeRecorder(1000),
				CreateTopoStore: newMemoryTopoFactory(),
			}
			if tc.reconcilerScheme != nil {
				reconciler.Scheme = tc.reconcilerScheme
			}

			// Create the Shard resource if not in existing objects
			shardInExisting := false
			for _, obj := range tc.existingObjects {
				if shard, ok := obj.(*multigresv1alpha1.Shard); ok && shard.Name == tc.shard.Name {
					shardInExisting = true
					break
				}
			}
			if !shardInExisting {
				err := fakeClient.Create(t.Context(), tc.shard)
				if err != nil {
					t.Fatalf("Failed to create Shard: %v", err)
				}
			}

			// Check headers
			for _, obj := range tc.existingObjects {
				if sts, ok := obj.(*appsv1.StatefulSet); ok {
					t.Logf(
						"PRERECONCILE STS: %s, Status.Replicas: %d, Status.ReadyReplicas: %d",
						sts.Name,
						sts.Status.Replicas,
						sts.Status.ReadyReplicas,
					)
				}
			}

			// Reconcile
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tc.shard.Name,
					Namespace: tc.shard.Namespace,
				},
			}

			// Reconcile in a loop to allow sequential pod creation to converge.
			// Each reconcile creates at most one pod per pool/cell combination.
			// After each iteration, restore the shard spec because the fake client's
			// SSA status patch implementation incorrectly clears unmanaged fields.
			const maxReconciles = 20
			for i := 0; i < maxReconciles; i++ {
				result, err := reconciler.Reconcile(t.Context(), req)
				if (err != nil) != tc.wantErr {
					t.Errorf("Reconcile() iteration %d error = %v, wantErr %v", i, err, tc.wantErr)
					return
				}
				if tc.wantErr {
					return
				}
				_ = result

				// Restore the shard spec: the fake client's SSA status patch
				// incorrectly clears spec fields that weren't in the patch object.
				stored := &multigresv1alpha1.Shard{}
				if err := fakeClient.Get(t.Context(), req.NamespacedName, stored); err == nil {
					stored.Spec = tc.shard.Spec
					_ = fakeClient.Update(t.Context(), stored)
				}

				// Mark all pods as Ready so the next iteration can create more pods.
				// The reconciler blocks creation when existing pods aren't Ready.
				podList := &corev1.PodList{}
				if err := fakeClient.List(
					t.Context(),
					podList,
					client.InNamespace(tc.shard.Namespace),
				); err == nil {
					for idx := range podList.Items {
						p := &podList.Items[idx]
						if isPodReady(p) {
							continue
						}
						p.Status.Conditions = []corev1.PodCondition{
							{Type: corev1.PodReady, Status: corev1.ConditionTrue},
						}
						_ = fakeClient.Status().Update(t.Context(), p)
					}
				}
			}

			// Run custom assertions if provided
			if tc.assertFunc != nil {
				tc.assertFunc(t, fakeClient, tc.shard)
			}
		})
	}
}

func TestShardReconciler_ReconcileNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	reconciler := &ShardReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(10),
		CreateTopoStore: newMemoryTopoFactory(),
	}

	// Reconcile non-existent resource
	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "nonexistent-shard",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Errorf("Reconcile() should not error on NotFound, got: %v", err)
	}
	if result.RequeueAfter > 0 {
		t.Errorf("Reconcile() should not requeue on NotFound")
	}
}

func TestShardReconciler_UpdateStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = multigresv1alpha1.AddToScheme(scheme)

	t.Run("all_replicas_ready_status", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-shard-ready",
				Namespace: "default",
				Labels: map[string]string{
					"multigres.com/cluster": "test-cluster",
				},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "testdb",
				TableGroupName: "default",
				Multiorch: multigresv1alpha1.MultiorchSpec{
					Cells: []multigresv1alpha1.CellName{"zone1"},
				},
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {
						Cells: []multigresv1alpha1.CellName{"zone1"},
						Type:  "readWrite",
						Storage: multigresv1alpha1.StorageSpec{
							Size: "10Gi",
						},
						ReplicasPerCell: ptr.To(int32(3)),
					},
				},
			},
		}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      BuildPoolPodName(shard, "primary", "zone1", 0),
				Namespace: "default",
				Labels: metadata.GetSelectorLabels(
					buildPoolLabelsWithCell(shard, "primary", "zone1"),
				),
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}

		moName := buildHashedMultiorchName(shard, "zone1")
		mo := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      moName,
				Namespace: "default",
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(int32(1)),
			},
			Status: appsv1.DeploymentStatus{
				Replicas:      1,
				ReadyReplicas: 1,
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod, mo).
			WithStatusSubresource(shard, pod, mo).
			Build()

		r := &ShardReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(100),
		}

		if err := r.updateStatus(context.Background(), shard); err != nil {
			t.Fatalf("updateStatus failed: %v", err)
		}

		updatedShard := &multigresv1alpha1.Shard{}
		if err := fakeClient.Get(
			context.Background(),
			client.ObjectKeyFromObject(shard),
			updatedShard,
		); err != nil {
			t.Fatalf("Failed to get Shard: %v", err)
		}

		foundTrue := false
		for _, cond := range updatedShard.Status.Conditions {
			if cond.Type == "Available" {
				if cond.Status != metav1.ConditionFalse {
					t.Errorf("Condition status = %s, want %s", cond.Status, metav1.ConditionFalse)
				}
				if cond.Reason != "NotAllPodsReady" {
					t.Errorf("Condition reason = %s, want %s", cond.Reason, "NotAllPodsReady")
				}
				foundTrue = true
			}
		}
		if !foundTrue {
			t.Errorf("Condition %s not found", "Available")
		}
		if updatedShard.Status.PoolsReady {
			t.Error("PoolsReady should be false when 1/3 pools are ready")
		}
		if updatedShard.Status.Phase != multigresv1alpha1.PhaseProgressing {
			t.Errorf(
				"Expected Phase to be %s, got %s",
				multigresv1alpha1.PhaseProgressing,
				updatedShard.Status.Phase,
			)
		}
	})

	t.Run("status_with_multiple_pools", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-shard-multi",
				Namespace: "default",
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "testdb",
				TableGroupName: "default",
				Multiorch: multigresv1alpha1.MultiorchSpec{
					Cells: []multigresv1alpha1.CellName{"zone1"},
				},
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"replica": {
						Cells:           []multigresv1alpha1.CellName{"zone1"},
						Type:            "replica",
						ReplicasPerCell: ptr.To(int32(2)),
					},
					"read-pool": {
						Cells:           []multigresv1alpha1.CellName{"zone1"},
						Type:            "readWrite",
						ReplicasPerCell: ptr.To(int32(3)),
					},
				},
			},
		}

		var objects []client.Object
		objects = append(objects, shard)
		for i := 0; i < 2; i++ {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      BuildPoolPodName(shard, "replica", "zone1", i),
					Namespace: "default",
					Labels: metadata.GetSelectorLabels(
						buildPoolLabelsWithCell(shard, "replica", "zone1"),
					),
				},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			}
			objects = append(objects, p)
		}
		for i := 0; i < 3; i++ {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      BuildPoolPodName(shard, "read-pool", "zone1", i),
					Namespace: "default",
					Labels: metadata.GetSelectorLabels(
						buildPoolLabelsWithCell(shard, "read-pool", "zone1"),
					),
				},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			}
			objects = append(objects, p)
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(objects...).
			WithStatusSubresource(objects...).
			Build()

		r := &ShardReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(100),
		}

		if err := r.updateStatus(context.Background(), shard); err != nil {
			t.Fatalf("updateStatus failed: %v", err)
		}

		updatedShard := &multigresv1alpha1.Shard{}
		if err := fakeClient.Get(
			context.Background(),
			client.ObjectKeyFromObject(shard),
			updatedShard,
		); err != nil {
			t.Fatalf("Failed to get Shard: %v", err)
		}

		if !updatedShard.Status.PoolsReady {
			t.Error("PoolsReady should be true when all pools are ready")
		}
	})

	t.Run("multi_cell_pool_aggregates_across_cells", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-shard-multicell",
				Namespace: "default",
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "testdb",
				TableGroupName: "default",
				Multiorch: multigresv1alpha1.MultiorchSpec{
					Cells: []multigresv1alpha1.CellName{"zone1", "zone2"},
				},
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {
						Cells:           []multigresv1alpha1.CellName{"zone1", "zone2"},
						Type:            "readWrite",
						ReplicasPerCell: ptr.To(int32(3)),
					},
				},
			},
		}

		var objects []client.Object
		objects = append(objects, shard)

		for i := 0; i < 3; i++ {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      BuildPoolPodName(shard, "primary", "zone1", i),
					Namespace: "default",
					Labels: metadata.GetSelectorLabels(
						buildPoolLabelsWithCell(shard, "primary", "zone1"),
					),
				},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			}
			objects = append(objects, p)
		}

		for i := 0; i < 2; i++ {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      BuildPoolPodName(shard, "primary", "zone2", i),
					Namespace: "default",
					Labels: metadata.GetSelectorLabels(
						buildPoolLabelsWithCell(shard, "primary", "zone2"),
					),
				},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			}
			objects = append(objects, p)
		}

		mo1Name := buildHashedMultiorchName(shard, "zone1")
		mo1 := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: mo1Name, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(int32(1))},
			Status:     appsv1.DeploymentStatus{Replicas: 1, ReadyReplicas: 1},
		}
		objects = append(objects, mo1)

		mo2Name := buildHashedMultiorchName(shard, "zone2")
		mo2 := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: mo2Name, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(int32(1))},
			Status:     appsv1.DeploymentStatus{Replicas: 1, ReadyReplicas: 1},
		}
		objects = append(objects, mo2)

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(objects...).
			WithStatusSubresource(objects...).
			Build()

		r := &ShardReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(100),
		}

		cellsSet := make(map[multigresv1alpha1.CellName]bool)
		totalPods, readyPods, _, err := r.updatePoolsStatus(
			context.Background(), shard, cellsSet,
		)
		if err != nil {
			t.Fatalf("updatePoolsStatus failed: %v", err)
		}

		// Verify aggregate: desired for primary is 3 pods per cell * 2 cells = 6 pods
		if totalPods != 6 {
			t.Errorf("totalPods = %d, want 6", totalPods)
		}
		if readyPods != 5 {
			t.Errorf("readyPods = %d, want 5", readyPods)
		}

		// Verify both cells are tracked
		if !cellsSet["zone1"] || !cellsSet["zone2"] {
			t.Errorf("cellsSet = %v, want both zone1 and zone2", cellsSet)
		}
	})
}

func TestScaleDownPodSelection(t *testing.T) {
	t.Parallel()
	r := &ShardReconciler{}

	shard := &multigresv1alpha1.Shard{
		Status: multigresv1alpha1.ShardStatus{
			PodRoles: map[string]string{
				"pod-0": "REPLICA",
				"pod-1": "PRIMARY",
				"pod-2": "REPLICA",
			},
		},
	}

	pods := []*corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-0"},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1"},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-2"},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		},
	}

	// 1. All ready, 1 is primary. Should pick 2 (highest index and not primary)
	selected := r.selectPodToDrain(context.Background(), pods, shard)
	if selected == nil || selected.Name != "pod-2" {
		t.Errorf("Expected pod-2 to be selected, got %v", selected)
	}

	// 2. Pod 0 is NOT ready. Should pick 0.
	pods[0].Status.Conditions[0].Status = corev1.ConditionFalse
	selected = r.selectPodToDrain(context.Background(), pods, shard)
	if selected == nil || selected.Name != "pod-0" {
		t.Errorf("Expected pod-0 (not ready) to be selected, got %v", selected)
	}

	// 3. Delete pod Roles, shouldn't panic, falls back to highest index
	shard.Status.PodRoles = nil
	pods[0].Status.Conditions[0].Status = corev1.ConditionTrue
	selected = r.selectPodToDrain(context.Background(), pods, shard)
	if selected == nil || selected.Name != "pod-2" {
		t.Errorf("Expected pod-2 (highest index) to be selected, got %v", selected)
	}
}

func TestScaleDown_ExternallyDeletedExtraPod(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					ReplicasPerCell: ptr.To(int32(2)), // Scale down to 2 replicas
					Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
				},
			},
		},
	}

	shardObj.Status.PodRoles = map[string]string{
		BuildPoolPodName(shardObj, "primary", "zone1", 0): "PRIMARY",
		BuildPoolPodName(shardObj, "primary", "zone1", 1): "REPLICA",
		BuildPoolPodName(shardObj, "primary", "zone1", 2): "REPLICA",
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
	poolSpec := shardObj.Spec.Pools["primary"]

	// Create 3 pods, simulating a scale-down from 3 to 2.
	for i := 0; i < 3; i++ {
		podName := BuildPoolPodName(shardObj, "primary", "zone1", i)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Labels: map[string]string{
					"app.kubernetes.io/component":     "shard-pool",
					"app.kubernetes.io/instance":      "test-cluster",
					metadata.LabelMultigresCluster:    "test-cluster",
					metadata.LabelMultigresDatabase:   "db",
					metadata.LabelMultigresTableGroup: "tg",
					metadata.LabelMultigresShard:      "s1",
					metadata.LabelMultigresPool:       "primary",
					metadata.LabelMultigresCell:       "zone1",
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}

		// Ensure proper spec hash to avoid drift deletion
		desiredPod, _ := BuildPoolPod(shardObj, "primary", "zone1", poolSpec, i, scheme)
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations[metadata.AnnotationSpecHash] = desiredPod.Annotations[metadata.AnnotationSpecHash]

		// For the extra pod (index 2), simulate an external deletion:
		// Give it a DeletionTimestamp.
		if i == 2 {
			pod.Finalizers = []string{"kubernetes.io/test"}
			now := metav1.Now()
			pod.DeletionTimestamp = &now
		}

		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatalf("failed to create pod: %v", err)
		}
	}

	// 1. Run reconcile loop
	err := r.reconcilePoolPods(context.Background(), shardObj, "primary", "zone1", poolSpec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 2. Extra pod with DeletionTimestamp should have drain requested by handleExternalDeletion
	var extraPod corev1.Pod
	err = c.Get(
		context.Background(),
		types.NamespacedName{
			Name:      BuildPoolPodName(shardObj, "primary", "zone1", 2),
			Namespace: "default",
		},
		&extraPod,
	)
	if err != nil {
		t.Fatalf("failed to get extra pod: %v", err)
	}

	if extraPod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
		t.Errorf(
			"Expected extra internally deleted pod to be marked for drain, got %v",
			extraPod.Annotations[metadata.AnnotationDrainState],
		)
	}
}

func TestScaleDown_HealthGateBlocksDrain(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
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

	poolName := "primary"
	cellName := "zone1"

	podName0 := BuildPoolPodName(shardObj, poolName, cellName, 0)
	podName1 := BuildPoolPodName(shardObj, poolName, cellName, 1)
	podName2 := BuildPoolPodName(shardObj, poolName, cellName, 2)

	makePod := func(name string, ready bool) *corev1.Pod {
		readyStatus := corev1.ConditionTrue
		if !ready {
			readyStatus = corev1.ConditionFalse
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   "default",
				Annotations: map[string]string{},
				Labels: map[string]string{
					metadata.LabelMultigresCell: cellName,
				},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: readyStatus},
				},
			},
		}
	}

	t.Run("blocks drain when pool has non-ready pod", func(t *testing.T) {
		t.Parallel()
		shard := shardObj.DeepCopy()
		shard.Status.PodRoles = map[string]string{
			podName0: "PRIMARY",
			podName1: "REPLICA",
			podName2: "REPLICA",
		}

		pods := []*corev1.Pod{
			makePod(podName0, true),
			makePod(podName1, false), // not ready
			makePod(podName2, true),
		}

		objects := []client.Object{shard}
		for _, p := range pods {
			objects = append(objects, p.DeepCopy())
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
		rec := record.NewFakeRecorder(10)
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: rec}

		existingPods := make(map[string]*corev1.Pod, len(pods))
		for _, p := range pods {
			existingPods[p.Name] = p
		}

		actionTaken, _, err := r.handleScaleDown(
			context.Background(), shard, poolName,
			multigresv1alpha1.PoolSpec{}, existingPods,
			2, // replicas: pod-2 is extra
			2, // effectiveReplicas
			false,
		)
		if err != nil {
			t.Fatalf("handleScaleDown returned error: %v", err)
		}

		if actionTaken {
			t.Error("Expected no action taken (health gate should block)")
		}

		// Pod-2 should NOT have drain annotation
		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			types.NamespacedName{Name: podName2, Namespace: "default"},
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updated.Annotations[metadata.AnnotationDrainState] != "" {
			t.Errorf(
				"Expected no drain annotation (health gate should block), got %q",
				updated.Annotations[metadata.AnnotationDrainState],
			)
		}

		// Verify ScaleDownBlocked event was emitted
		select {
		case event := <-rec.Events:
			if !strings.Contains(event, "ScaleDownBlocked") {
				t.Errorf("Expected ScaleDownBlocked event, got %q", event)
			}
		default:
			t.Error("Expected ScaleDownBlocked event to be emitted")
		}
	})

	t.Run("allows drain when all pods are healthy", func(t *testing.T) {
		t.Parallel()
		shard := shardObj.DeepCopy()
		shard.Status.PodRoles = map[string]string{
			podName0: "PRIMARY",
			podName1: "REPLICA",
			podName2: "REPLICA",
		}

		pods := []*corev1.Pod{
			makePod(podName0, true),
			makePod(podName1, true),
			makePod(podName2, true),
		}

		objects := []client.Object{shard}
		for _, p := range pods {
			objects = append(objects, p.DeepCopy())
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := make(map[string]*corev1.Pod, len(pods))
		for _, p := range pods {
			existingPods[p.Name] = p
		}

		actionTaken, _, err := r.handleScaleDown(
			context.Background(), shard, poolName,
			multigresv1alpha1.PoolSpec{}, existingPods,
			2, // replicas: pod-2 is extra
			2, // effectiveReplicas
			false,
		)
		if err != nil {
			t.Fatalf("handleScaleDown returned error: %v", err)
		}

		if !actionTaken {
			t.Error("Expected action taken (drain should proceed)")
		}

		// Pod-2 SHOULD have drain annotation
		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			types.NamespacedName{Name: podName2, Namespace: "default"},
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updated.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
			t.Errorf(
				"Expected drain annotation %q, got %q",
				metadata.DrainStateRequested,
				updated.Annotations[metadata.AnnotationDrainState],
			)
		}
	})

	t.Run("unhealthy extra pod does not block its own removal", func(t *testing.T) {
		t.Parallel()
		shard := shardObj.DeepCopy()
		shard.Status.PodRoles = map[string]string{
			podName0: "PRIMARY",
			podName1: "REPLICA",
			podName2: "REPLICA",
		}

		pods := []*corev1.Pod{
			makePod(podName0, true),
			makePod(podName1, true),
			makePod(podName2, false), // extra pod is unhealthy (e.g. CrashLoopBackOff)
		}

		objects := []client.Object{shard}
		for _, p := range pods {
			objects = append(objects, p.DeepCopy())
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		existingPods := make(map[string]*corev1.Pod, len(pods))
		for _, p := range pods {
			existingPods[p.Name] = p
		}

		actionTaken, _, err := r.handleScaleDown(
			context.Background(), shard, poolName,
			multigresv1alpha1.PoolSpec{}, existingPods,
			2, // replicas: pod-2 is extra
			2, // effectiveReplicas
			false,
		)
		if err != nil {
			t.Fatalf("handleScaleDown returned error: %v", err)
		}

		if !actionTaken {
			t.Error("Expected action taken (unhealthy extra pod should not block its own removal)")
		}

		// Pod-2 SHOULD have drain annotation despite being unhealthy
		updated := &corev1.Pod{}
		if err := c.Get(
			context.Background(),
			types.NamespacedName{Name: podName2, Namespace: "default"},
			updated,
		); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updated.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
			t.Errorf(
				"Expected drain annotation %q, got %q",
				metadata.DrainStateRequested,
				updated.Annotations[metadata.AnnotationDrainState],
			)
		}
	})
}

func TestRollingUpdateOrder(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					ReplicasPerCell: ptr.To(int32(3)),
					Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
				},
			},
		},
		Status: multigresv1alpha1.ShardStatus{
			// PodRoles to be assigned later
		},
	}

	shardObj.Status.PodRoles = map[string]string{
		BuildPoolPodName(shardObj, "primary", "zone1", 0): "REPLICA",
		BuildPoolPodName(shardObj, "primary", "zone1", 1): "PRIMARY",
		BuildPoolPodName(shardObj, "primary", "zone1", 2): "REPLICA",
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
	poolSpec := shardObj.Spec.Pools["primary"]

	// Create 3 pods, all with old spec hash
	for i := 0; i < 3; i++ {
		podName := BuildPoolPodName(shardObj, "primary", "zone1", i)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Labels: map[string]string{
					"app.kubernetes.io/component":     "shard-pool",
					"app.kubernetes.io/instance":      "test-cluster",
					metadata.LabelMultigresCluster:    "test-cluster",
					metadata.LabelMultigresDatabase:   "db",
					metadata.LabelMultigresTableGroup: "tg",
					metadata.LabelMultigresShard:      "s1",
					metadata.LabelMultigresPool:       "primary",
					metadata.LabelMultigresCell:       "zone1",
				},
				Annotations: map[string]string{
					metadata.AnnotationSpecHash: "old-hash",
				},
			},
		}
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatalf("failed to create pod: %v", err)
		}
	}

	// Run reconcile loop
	err := r.reconcilePoolPods(context.Background(), shardObj, "primary", "zone1", poolSpec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that EXACTLY ONE REPLICA has drain-requested annotation. Pod 1 is PRIMARY.
	// So either Pod 0 or Pod 2 should be marked for drain, not Pod 1.
	drainCount := 0
	for i := 0; i < 3; i++ {
		var pod corev1.Pod
		if err := c.Get(
			context.Background(),
			types.NamespacedName{
				Name:      BuildPoolPodName(shardObj, "primary", "zone1", i),
				Namespace: "default",
			},
			&pod,
		); err != nil {
			t.Fatalf("pod %d should still exist: %v", i, err)
		}
		if pod.Annotations[metadata.AnnotationDrainState] == metadata.DrainStateRequested {
			drainCount++
			if i == 1 {
				t.Errorf("Primary pod was marked for drain before replicas!")
			}
		}
	}
	if drainCount != 1 {
		t.Errorf("Expected exactly 1 pod to have drain-requested annotation, got %d", drainCount)
	}
}

func TestDrainedPodReplacement(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					ReplicasPerCell: ptr.To(int32(1)),
					Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
				},
			},
		},
	}

	podName0 := BuildPoolPodName(shardObj, "primary", "zone1", 0)
	shardObj.Status.PodRoles = map[string]string{
		podName0: "DRAINED",
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName0,
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/component":     "shard-pool",
				"app.kubernetes.io/instance":      "test-cluster",
				metadata.LabelMultigresCluster:    "test-cluster",
				metadata.LabelMultigresDatabase:   "db",
				metadata.LabelMultigresTableGroup: "tg",
				metadata.LabelMultigresShard:      "s1",
				metadata.LabelMultigresPool:       "primary",
				metadata.LabelMultigresCell:       "zone1",
				metadata.LabelPodRole:             "DRAINED",
			},
			Annotations: map[string]string{},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}

	// Pre-calculate hash so it doesn't get deleted as drifted
	poolSpec := shardObj.Spec.Pools["primary"]
	desiredPod, _ := BuildPoolPod(shardObj, "primary", "zone1", poolSpec, 0, scheme)
	pod.Annotations[metadata.AnnotationSpecHash] = desiredPod.Annotations[metadata.AnnotationSpecHash]

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	// Run reconcile loop
	err := r.reconcilePoolPods(context.Background(), shardObj, "primary", "zone1", poolSpec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The DRAINED pod should NOT have a drain annotation (DRAINED pods are no longer auto-drained)
	err = c.Get(
		context.Background(),
		types.NamespacedName{Name: podName0, Namespace: "default"},
		pod,
	)
	if err != nil {
		t.Fatalf("expected pod to exist, got %v", err)
	}

	if pod.Annotations[metadata.AnnotationDrainState] != "" {
		t.Errorf(
			"Expected DRAINED pod to have no drain annotation, got %q",
			pod.Annotations[metadata.AnnotationDrainState],
		)
	}

	// A stand-in pod at index 1 should be created (replicas=1, effectiveReplicas=2)
	podName1 := BuildPoolPodName(shardObj, "primary", "zone1", 1)
	standInPod := &corev1.Pod{}
	err = c.Get(
		context.Background(),
		types.NamespacedName{Name: podName1, Namespace: "default"},
		standInPod,
	)
	if err != nil {
		t.Fatalf("expected stand-in pod at index 1 to be created, got %v", err)
	}
}
