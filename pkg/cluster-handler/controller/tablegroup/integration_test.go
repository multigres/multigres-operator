//go:build integration
// +build integration

package tablegroup_test

import (
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
)

func TestSetupWithManager(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(
			filepath.Join("../../../../", "config", "crd", "bases"),
		),
	)

	if err := (&tablegroup.TableGroupReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
	}).SetupWithManager(mgr, controller.Options{
		SkipNameValidation: ptr.To(true),
	}); err != nil {
		t.Fatalf("Failed to create controller, %v", err)
	}
}

func TestSetupWithManager_Failure(t *testing.T) {
	t.Parallel()

	// Scheme WITHOUT Multigres types serves to trigger a failure in SetupWithManager
	// because controller-runtime cannot map the Go type to a GVK.
	scheme := runtime.NewScheme()
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(
			filepath.Join("../../../../", "config", "crd", "bases"),
		),
	)

	if err := (&tablegroup.TableGroupReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
	}).SetupWithManager(mgr, controller.Options{
		SkipNameValidation: ptr.To(true),
	}); err == nil {
		t.Fatal("Expected SetupWithManager to fail due to missing type in scheme, got nil")
	}
}

func setTestPostgresPasswordSecretRef(tableGroup *multigresv1alpha1.TableGroup) {
	if tableGroup == nil || tableGroup.Spec.PostgresPasswordSecretRef.Name != "" {
		return
	}
	tableGroup.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
		Name: "multigres-admin-password",
		Key:  "password",
	}
}

func setTestShardPostgresPasswordSecretRef(shard *multigresv1alpha1.Shard) {
	if shard == nil || shard.Spec.PostgresPasswordSecretRef.Name != "" {
		return
	}
	shard.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
		Name: "multigres-admin-password",
		Key:  "password",
	}
}

func TestTableGroupReconciliation(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		tableGroup    *multigresv1alpha1.TableGroup
		wantResources []client.Object
	}{
		"simple tablegroup creates shards": {
			tableGroup: &multigresv1alpha1.TableGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "tg-test-simple",
					Namespace: "default",
					Labels: map[string]string{
						"multigres.com/cluster":    "test-cluster",
						"multigres.com/database":   "db1",
						"multigres.com/tablegroup": "tg1",
					},
				},
				Spec: multigresv1alpha1.TableGroupSpec{
					DatabaseName:   "db1",
					TableGroupName: "tg1",
					Images: multigresv1alpha1.ShardImages{
						Multiorch:   "orch:latest",
						Multipooler: "pooler:latest",
						Postgres:    "postgres:15",
					},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "etcd:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					Shards: []multigresv1alpha1.ShardResolvedSpec{
						{
							Name: "s1",
							Multiorch: multigresv1alpha1.MultiorchSpec{
								StatelessSpec: multigresv1alpha1.StatelessSpec{
									Replicas: ptr.To(int32(1)),
								},
								Cells: []multigresv1alpha1.CellName{"zone-a"},
							},
							Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
								"primary": {
									Type:            "readWrite",
									ReplicasPerCell: ptr.To(int32(1)),
									Cells:           []multigresv1alpha1.CellName{"zone-a"},
								},
							},
						},
						{
							Name: "s2",
							Multiorch: multigresv1alpha1.MultiorchSpec{
								StatelessSpec: multigresv1alpha1.StatelessSpec{
									Replicas: ptr.To(int32(1)),
								},
								Cells: []multigresv1alpha1.CellName{"zone-b"},
							},
							Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
								"primary": {
									Type:            "readWrite",
									ReplicasPerCell: ptr.To(int32(1)),
									Cells:           []multigresv1alpha1.CellName{"zone-b"},
								},
							},
						},
					},
				},
			},
			wantResources: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "tg-test-simple-s1",
						Namespace:       "default",
						Labels:          shardLabels("test-cluster", "db1", "tg1", "s1"),
						OwnerReferences: tgOwnerRefs(t, "tg-test-simple"),
					},
					Spec: multigresv1alpha1.ShardSpec{
						ShardName:      "s1",
						DatabaseName:   "db1",
						TableGroupName: "tg1",
						Images: multigresv1alpha1.ShardImages{
							Multiorch:   "orch:latest",
							Multipooler: "pooler:latest",
							Postgres:    "postgres:15",
						},
						GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
							Address:        "etcd:2379",
							RootPath:       "/multigres/global",
							Implementation: "etcd",
						},
						Multiorch: multigresv1alpha1.MultiorchSpec{
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Replicas: ptr.To(int32(1)),
							},
							Cells: []multigresv1alpha1.CellName{"zone-a"},
						},
						Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
							"primary": {
								Type:            "readWrite",
								ReplicasPerCell: ptr.To(int32(1)),
								Cells:           []multigresv1alpha1.CellName{"zone-a"},
							},
						},
						Replicas: ptr.To(int32(1)),
					},
				},
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "tg-test-simple-s2",
						Namespace:       "default",
						Labels:          shardLabels("test-cluster", "db1", "tg1", "s2"),
						OwnerReferences: tgOwnerRefs(t, "tg-test-simple"),
					},
					Spec: multigresv1alpha1.ShardSpec{
						ShardName:      "s2",
						DatabaseName:   "db1",
						TableGroupName: "tg1",
						Images: multigresv1alpha1.ShardImages{
							Multiorch:   "orch:latest",
							Multipooler: "pooler:latest",
							Postgres:    "postgres:15",
						},
						GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
							Address:        "etcd:2379",
							RootPath:       "/multigres/global",
							Implementation: "etcd",
						},
						Multiorch: multigresv1alpha1.MultiorchSpec{
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Replicas: ptr.To(int32(1)),
							},
							Cells: []multigresv1alpha1.CellName{"zone-b"},
						},
						Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
							"primary": {
								Type:            "readWrite",
								ReplicasPerCell: ptr.To(int32(1)),
								Cells:           []multigresv1alpha1.CellName{"zone-b"},
							},
						},
						Replicas: ptr.To(int32(1)),
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()

			mgr := testutil.SetUpEnvtestManager(t, scheme,
				testutil.WithCRDPaths(
					filepath.Join("../../../../", "config", "crd", "bases"),
				),
			)

			watcher := testutil.NewResourceWatcher(t, ctx, mgr,
				testutil.WithCmpOpts(
					testutil.IgnoreMetaRuntimeFields(),
				),
				testutil.WithExtraResource(
					&multigresv1alpha1.TableGroup{},
					&multigresv1alpha1.Shard{},
				),
				testutil.WithTimeout(10*time.Second),
			)
			k8sClient := mgr.GetClient()

			reconciler := &tablegroup.TableGroupReconciler{
				Client:   mgr.GetClient(),
				Scheme:   mgr.GetScheme(),
				Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
			}

			if err := reconciler.SetupWithManager(mgr, controller.Options{
				SkipNameValidation: ptr.To(true),
			}); err != nil {
				t.Fatalf("Failed to create controller, %v", err)
			}

			setTestPostgresPasswordSecretRef(tc.tableGroup)
			if err := k8sClient.Create(ctx, tc.tableGroup); err != nil {
				t.Fatalf("Failed to create the initial tablegroup, %v", err)
			}

			// Expected Shard names use the same name constraints as the controller.
			for _, obj := range tc.wantResources {
				if shard, ok := obj.(*multigresv1alpha1.Shard); ok {
					clusterName := tc.tableGroup.Labels["multigres.com/cluster"]
					expectedName := nameutil.JoinWithConstraints(
						nameutil.DefaultConstraints,
						clusterName,
						string(tc.tableGroup.Spec.DatabaseName),
						string(tc.tableGroup.Spec.TableGroupName),
						string(shard.Spec.ShardName),
					)
					shard.Name = expectedName
					setTestShardPostgresPasswordSecretRef(shard)
				}
			}

			if err := watcher.WaitForMatch(tc.wantResources...); err != nil {
				t.Errorf("Resources mismatch:\n%v", err)
			}
		})
	}
}

func shardLabels(clusterName, db, tg, shard string) map[string]string {
	labels := metadata.BuildStandardLabels(clusterName, "shard")
	metadata.AddClusterLabel(labels, clusterName)
	metadata.AddDatabaseLabel(labels, multigresv1alpha1.DatabaseName(db))
	metadata.AddTableGroupLabel(labels, multigresv1alpha1.TableGroupName(tg))
	metadata.AddShardLabel(labels, multigresv1alpha1.ShardName(shard))
	return labels
}

func tgOwnerRefs(t testing.TB, tgName string) []metav1.OwnerReference {
	t.Helper()
	return []metav1.OwnerReference{{
		APIVersion:         "multigres.com/v1alpha1",
		Kind:               "TableGroup",
		Name:               tgName,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}}
}
