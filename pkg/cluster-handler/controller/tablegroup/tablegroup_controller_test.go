package tablegroup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

func setupFixtures(
	t testing.TB,
) (*multigresv1alpha1.TableGroup, string, string, string, string, string) {
	t.Helper()

	tgName := "test-tg"
	namespace := "default"
	clusterName := "test-cluster"
	dbName := "db1"
	tgLabelName := "tg1"

	baseTG := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tgName,
			Namespace: namespace,
			Labels: map[string]string{
				"multigres.com/cluster":    clusterName,
				"multigres.com/database":   dbName,
				"multigres.com/tablegroup": tgLabelName,
			},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   multigresv1alpha1.DatabaseName(dbName),
			TableGroupName: multigresv1alpha1.TableGroupName(tgLabelName),
			Images: multigresv1alpha1.ShardImages{
				MultiOrch:   "orch:v1",
				MultiPooler: "pooler:v1",
				Postgres:    "pg:15",
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address: "http://etcd:2379",
			},
			Shards: []multigresv1alpha1.ShardResolvedSpec{
				{
					Name: "shard-0",
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						StatelessSpec: multigresv1alpha1.StatelessSpec{
							Replicas: ptr.To(int32(1)),
						},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"data": {ReplicasPerCell: ptr.To(int32(2))},
					},
				},
			},
		},
	}
	return baseTG, tgName, namespace, clusterName, dbName, tgLabelName
}

func TestTableGroupReconciler_Reconcile_Success(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	baseTG, tgName, namespace, clusterName, dbName, tgLabelName := setupFixtures(t)

	tests := map[string]struct {
		tableGroup         *multigresv1alpha1.TableGroup
		existingObjects    []client.Object
		preReconcileUpdate func(testing.TB, *multigresv1alpha1.TableGroup)
		preReconcileClient func(testing.TB, client.Client) // Hook to modify client state before Reconcile
		skipCreate         bool                            // If true, the object won't be created in the fake client (simulates Not Found)
		expectedEvents     []string                        // events expected to be recorded
		validate           func(testing.TB, client.Client)
	}{
		"Create: Shard Creation": {
			tableGroup:      baseTG.DeepCopy(),
			existingObjects: []client.Object{},
			expectedEvents: []string{
				"Normal Applied Applied Shard",
				"Normal Synced Successfully reconciled TableGroup",
			},
			validate: func(t testing.TB, c client.Client) {
				ctx := t.Context()
				// The name is the md5 hash of test-cluster, db1, tg1, shard-0.
				shardNameFull := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					dbName,
					tgLabelName,
					"shard-0",
				)
				shard := &multigresv1alpha1.Shard{}
				if err := c.Get(
					ctx,
					types.NamespacedName{Name: shardNameFull, Namespace: namespace},
					shard,
				); err != nil {
					t.Fatalf("Shard %s not created: %v", shardNameFull, err)
				}
				if got, want := shard.Spec.DatabaseName, multigresv1alpha1.DatabaseName(
					dbName,
				); got != want {
					t.Errorf("Shard DB name mismatch got %q, want %q", got, want)
				}
			},
		},

		"Status: Update Ready Count": {
			tableGroup: baseTG.DeepCopy(),
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
					Status: multigresv1alpha1.ShardStatus{
						Phase: multigresv1alpha1.PhaseHealthy,
						Conditions: []metav1.Condition{
							{
								Type:               "Available",
								Status:             metav1.ConditionTrue,
								LastTransitionTime: metav1.Now(),
							},
						},
					},
				},
			},
			expectedEvents: []string{
				"Normal Applied Applied Shard",
				"Normal Synced Successfully reconciled TableGroup",
			},
			validate: func(t testing.TB, c client.Client) {
				updatedTG := &multigresv1alpha1.TableGroup{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: tgName, Namespace: namespace},
					updatedTG,
				); err != nil {
					t.Fatalf("failed to get tablegroup: %v", err)
				}
				if got, want := updatedTG.Status.ReadyShards, int32(1); got != want {
					t.Errorf("ReadyShards mismatch got %d, want %d", got, want)
				}
			},
		},
		"Status: Partial Ready (Not all shards ready)": {
			tableGroup: baseTG.DeepCopy(),
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-%s", tgName, "shard-0"),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
					// No status, so not ready
				},
			},
			validate: func(t testing.TB, c client.Client) {
				updatedTG := &multigresv1alpha1.TableGroup{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: tgName, Namespace: namespace},
					updatedTG,
				); err != nil {
					t.Fatalf("failed to get tablegroup: %v", err)
				}
				if got, want := updatedTG.Status.ReadyShards, int32(0); got != want {
					t.Errorf("ReadyShards mismatch got %d, want %d", got, want)
				}
				if meta.IsStatusConditionTrue(updatedTG.Status.Conditions, "Available") {
					t.Error("TableGroup should NOT be Available")
				}
			},
		},
		"Status: Shard Not Ready (False Condition)": {
			tableGroup: baseTG.DeepCopy(),
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-%s", tgName, "shard-0"),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
					Status: multigresv1alpha1.ShardStatus{
						Conditions: []metav1.Condition{
							{
								Type:   "Available",
								Status: metav1.ConditionFalse,
							},
							{
								Type:   "SomethingElse",
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
			},
			validate: func(t testing.TB, c client.Client) {
				updatedTG := &multigresv1alpha1.TableGroup{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: tgName, Namespace: namespace},
					updatedTG,
				); err != nil {
					t.Fatalf("failed to get tablegroup: %v", err)
				}
				if got, want := updatedTG.Status.ReadyShards, int32(0); got != want {
					t.Errorf("ReadyShards mismatch got %d, want %d", got, want)
				}
			},
		},
		"Status: Degraded Shard": {
			tableGroup: baseTG.DeepCopy(),
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
					Status: multigresv1alpha1.ShardStatus{
						Phase: multigresv1alpha1.PhaseDegraded,
					},
				},
			},
			validate: func(t testing.TB, c client.Client) {
				updatedTG := &multigresv1alpha1.TableGroup{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: tgName, Namespace: namespace},
					updatedTG,
				); err != nil {
					t.Fatalf("failed to get tablegroup: %v", err)
				}
				if got, want := updatedTG.Status.Phase, multigresv1alpha1.PhaseDegraded; got != want {
					t.Errorf("Phase mismatch got %q, want %q", got, want)
				}
			},
		},
		"Status: Zero Shards (Vacuously True)": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{}
			},
			existingObjects: []client.Object{},
			validate: func(t testing.TB, c client.Client) {
				updatedTG := &multigresv1alpha1.TableGroup{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: tgName, Namespace: namespace},
					updatedTG,
				); err != nil {
					t.Fatalf("failed to get tablegroup: %v", err)
				}
				if !meta.IsStatusConditionTrue(updatedTG.Status.Conditions, "Available") {
					t.Error("Zero shard TableGroup should be Available")
				}
			},
		},

		"Error: Object Not Found (Clean Exit)": {
			tableGroup:      baseTG.DeepCopy(),
			skipCreate:      true,
			existingObjects: []client.Object{},
		},
		"Update: Shard Update Success": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				tg.Spec.Shards[0].MultiOrch.Replicas = ptr.To(int32(5))
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
				},
			},
			validate: func(t testing.TB, c client.Client) {
				shard := &multigresv1alpha1.Shard{}
				shardName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					dbName,
					tgLabelName,
					"shard-0",
				)
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: shardName, Namespace: namespace},
					shard,
				); err != nil {
					t.Fatal(err)
				}
				if *shard.Spec.MultiOrch.Replicas != 5 {
					t.Errorf("Shard replicas not updated")
				}
			},
		},
		"Success: Early Return on Deletion": {
			tableGroup:      baseTG.DeepCopy(),
			existingObjects: []client.Object{},
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				now := metav1.Now()
				tg.DeletionTimestamp = &now
				tg.Finalizers = []string{
					"test.finalizer",
				}
			},
			validate: func(t testing.TB, c client.Client) {
				shardNameFull := fmt.Sprintf("%s-%s", tgName, "shard-0")
				shard := &multigresv1alpha1.Shard{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: shardNameFull, Namespace: namespace},
					shard,
				); !apierrors.IsNotFound(
					err,
				) {
					t.Errorf("Expected Shard %s to NOT be created", shardNameFull)
				}
			},
		},
		"Success: Prune Orphan Shard (Sets PendingDeletion)": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{}
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
				},
			},
			expectedEvents: []string{
				"Normal PendingDeletion Marked Shard",
			},
			validate: func(t testing.TB, c client.Client) {
				shardName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					dbName,
					tgLabelName,
					"shard-0",
				)
				shard := &multigresv1alpha1.Shard{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: shardName, Namespace: namespace},
					shard,
				); err != nil {
					t.Fatalf("Shard should still exist: %v", err)
				}
				if shard.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
					t.Error("Expected PendingDeletion annotation to be set")
				}
			},
		},

		"Success: Prune Orphan Shard (Waits For Drain)": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{}
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
						Annotations: map[string]string{
							multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
						},
					},
					Spec:   multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
					Status: multigresv1alpha1.ShardStatus{},
				},
			},
			validate: func(t testing.TB, c client.Client) {
				// Shard should still exist because ReadyForDeletion is not True.
				shardName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					dbName,
					tgLabelName,
					"shard-0",
				)
				shard := &multigresv1alpha1.Shard{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: shardName, Namespace: namespace},
					shard,
				); err != nil {
					t.Fatalf("Shard should still exist while draining: %v", err)
				}
			},
		},

		"Success: Prune Orphan Shard (Deletes After ReadyForDeletion)": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{}
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
						Annotations: map[string]string{
							multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
					Status: multigresv1alpha1.ShardStatus{
						Conditions: []metav1.Condition{
							{
								Type:               multigresv1alpha1.ConditionReadyForDeletion,
								Status:             metav1.ConditionTrue,
								Reason:             "DrainComplete",
								LastTransitionTime: metav1.Now(),
							},
						},
					},
				},
			},
			expectedEvents: []string{
				"Normal Deleted Deleted orphaned Shard",
			},
			validate: func(t testing.TB, c client.Client) {
				shardName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					dbName,
					tgLabelName,
					"shard-0",
				)
				shard := &multigresv1alpha1.Shard{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: shardName, Namespace: namespace},
					shard,
				); !apierrors.IsNotFound(err) {
					t.Errorf("Expected Shard %s to be deleted", shardName)
				}
			},
		},

		"Success: Build Shard (Long Name - Truncated)": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				tg.Spec.Shards[0].Name = "a" + string(make([]byte, 64)) // > 63 chars
			},
			existingObjects: []client.Object{},
		},
		"Success: Handle Pending Deletion (Propagate to Shards)": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				if tg.Annotations == nil {
					tg.Annotations = make(map[string]string)
				}
				tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = "2026-01-01T00:00:00Z"
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
				},
			},
			expectedEvents: []string{
				"Normal PendingDeletion Marked Shard",
			},
			validate: func(t testing.TB, c client.Client) {
				shardName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					dbName,
					tgLabelName,
					"shard-0",
				)
				shard := &multigresv1alpha1.Shard{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: shardName, Namespace: namespace},
					shard,
				); err != nil {
					t.Fatalf("Shard should exist: %v", err)
				}
				if shard.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
					t.Error("Expected PendingDeletion annotation to be set on Shard")
				}
			},
		},
		"Success: Handle Pending Deletion (All Shards Ready For Deletion)": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				if tg.Annotations == nil {
					tg.Annotations = make(map[string]string)
				}
				tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = "2026-01-01T00:00:00Z"
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
						Annotations: map[string]string{
							multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
					Status: multigresv1alpha1.ShardStatus{
						Conditions: []metav1.Condition{
							{
								Type:               multigresv1alpha1.ConditionReadyForDeletion,
								Status:             metav1.ConditionTrue,
								Reason:             "DrainComplete",
								LastTransitionTime: metav1.Now(),
							},
						},
					},
				},
			},
			expectedEvents: []string{
				"Normal ReadyForDeletion TableGroup test-tg marked ready for deletion",
			},
			validate: func(t testing.TB, c client.Client) {
				updatedTG := &multigresv1alpha1.TableGroup{}
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: tgName, Namespace: namespace},
					updatedTG,
				); err != nil {
					t.Fatalf("failed to get tablegroup: %v", err)
				}
				if !meta.IsStatusConditionTrue(
					updatedTG.Status.Conditions,
					multigresv1alpha1.ConditionReadyForDeletion,
				) {
					t.Error("TableGroup should be ReadyForDeletion")
				}
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if tc.preReconcileUpdate != nil {
				tc.preReconcileUpdate(t, tc.tableGroup)
			}

			objects := tc.existingObjects
			if !tc.skipCreate {
				objects = append(objects, tc.tableGroup)
			}

			clientBuilder := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				WithStatusSubresource(&multigresv1alpha1.TableGroup{}, &multigresv1alpha1.Shard{})
			baseClient := clientBuilder.Build()

			if tc.preReconcileClient != nil {
				tc.preReconcileClient(t, baseClient)
			}

			recorder := record.NewFakeRecorder(100)

			reconciler := &TableGroupReconciler{
				Client:   baseClient,
				Scheme:   scheme,
				Recorder: recorder,
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tc.tableGroup.Name,
					Namespace: tc.tableGroup.Namespace,
				},
			}

			_, err := reconciler.Reconcile(t.Context(), req)
			if err != nil {
				t.Errorf("Unexpected error from Reconcile: %v", err)
			}

			if len(tc.expectedEvents) > 0 {
				close(recorder.Events)
				var gotEvents []string
				for evt := range recorder.Events {
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

func TestTableGroupReconciler_DefaultMVPShape(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	const (
		tgName      = "default-tablegroup"
		namespace   = "default"
		clusterName = "test-cluster"
		dbName      = "postgres"
		tgLabelName = "default"
		shardName   = "0-inf"
	)

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tgName,
			Namespace: namespace,
			Labels: map[string]string{
				"multigres.com/cluster":    clusterName,
				"multigres.com/database":   dbName,
				"multigres.com/tablegroup": tgLabelName,
			},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   multigresv1alpha1.DatabaseName(dbName),
			TableGroupName: multigresv1alpha1.TableGroupName(tgLabelName),
			IsDefault:      true,
			Images: multigresv1alpha1.ShardImages{
				MultiOrch:   "orch:v1",
				MultiPooler: "pooler:v1",
				Postgres:    "pg:15",
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address: "http://etcd:2379",
			},
			Shards: []multigresv1alpha1.ShardResolvedSpec{
				{
					Name: shardName,
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						StatelessSpec: multigresv1alpha1.StatelessSpec{
							Replicas: ptr.To(int32(1)),
						},
						Cells: []multigresv1alpha1.CellName{"zone-a"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"default": {
							Type:            "readWrite",
							ReplicasPerCell: ptr.To(int32(1)),
							Cells:           []multigresv1alpha1.CellName{"zone-a"},
						},
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tg).
		WithStatusSubresource(
			&multigresv1alpha1.TableGroup{},
			&multigresv1alpha1.Shard{},
		).
		Build()

	reconciler := &TableGroupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: tgName, Namespace: namespace},
	}

	if got, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("unexpected error creating default shard: %v", err)
	} else if got != (ctrl.Result{}) {
		t.Fatalf("unexpected reconcile result creating default shard: got %+v", got)
	}

	fullShardName := name.JoinWithConstraints(
		name.DefaultConstraints,
		clusterName,
		dbName,
		tgLabelName,
		shardName,
	)
	shard := &multigresv1alpha1.Shard{}
	if err := c.Get(
		t.Context(),
		types.NamespacedName{Name: fullShardName, Namespace: namespace},
		shard,
	); err != nil {
		t.Fatalf("default 0-inf Shard was not created: %v", err)
	}

	if got, want := shard.Spec.DatabaseName, multigresv1alpha1.DatabaseName(dbName); got != want {
		t.Errorf("Shard DatabaseName mismatch: got %q, want %q", got, want)
	}
	if got, want := shard.Spec.TableGroupName, multigresv1alpha1.TableGroupName(tgLabelName); got != want {
		t.Errorf("Shard TableGroupName mismatch: got %q, want %q", got, want)
	}
	if got, want := shard.Spec.ShardName, multigresv1alpha1.ShardName(shardName); got != want {
		t.Errorf("ShardName mismatch: got %q, want %q", got, want)
	}
	if got, want := shard.Labels["multigres.com/tablegroup"], tgLabelName; got != want {
		t.Errorf("tablegroup label mismatch: got %q, want %q", got, want)
	}

	healthyTG := tg.DeepCopy()
	healthyShard := shard.DeepCopy()
	healthyShard.Status.Phase = multigresv1alpha1.PhaseHealthy
	healthyShard.Status.ObservedGeneration = healthyShard.Generation

	c = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(healthyShard, healthyTG).
		WithStatusSubresource(
			&multigresv1alpha1.TableGroup{},
			&multigresv1alpha1.Shard{},
		).
		Build()

	reconciler.Client = c
	if got, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("unexpected error reconciling healthy default shard: %v", err)
	} else if got != (ctrl.Result{}) {
		t.Fatalf("unexpected reconcile result for healthy default shard: got %+v", got)
	}

	updatedTG := &multigresv1alpha1.TableGroup{}
	if err := c.Get(
		t.Context(),
		types.NamespacedName{Name: tgName, Namespace: namespace},
		updatedTG,
	); err != nil {
		t.Fatalf("failed to get TableGroup: %v", err)
	}

	if got, want := updatedTG.Status.Phase, multigresv1alpha1.PhaseHealthy; got != want {
		t.Errorf("TableGroup phase mismatch: got %q, want %q", got, want)
	}
	if got, want := updatedTG.Status.ReadyShards, int32(1); got != want {
		t.Errorf("ReadyShards mismatch: got %d, want %d", got, want)
	}
	if got, want := updatedTG.Status.TotalShards, int32(1); got != want {
		t.Errorf("TotalShards mismatch: got %d, want %d", got, want)
	}
	if !meta.IsStatusConditionTrue(updatedTG.Status.Conditions, "Available") {
		t.Error("default 0-inf TableGroup should be Available after its shard is healthy")
	}
}

func TestTableGroupReconciler_Reconcile_Failure(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	baseTG, tgName, namespace, clusterName, dbName, tgLabelName := setupFixtures(t)
	errSimulated := errors.New("simulated error for testing")

	tests := map[string]struct {
		tableGroup         *multigresv1alpha1.TableGroup
		existingObjects    []client.Object
		preReconcileUpdate func(testing.TB, *multigresv1alpha1.TableGroup)
		failureConfig      *testutil.FailureConfig
		expectedEvents     []string
	}{
		"Error: Get TableGroup Failed": {
			tableGroup:      baseTG.DeepCopy(),
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnKeyName(tgName, errSimulated),
			},
			// Fails before any event recording
		},
		"Error: Apply Shard Failed": {
			tableGroup:      baseTG.DeepCopy(),
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(
					name.JoinWithConstraints(
						name.DefaultConstraints,
						clusterName,
						dbName,
						tgLabelName,
						"shard-0",
					),
					errSimulated,
				),
			},
			expectedEvents: []string{"Warning FailedApply Failed to apply shard"},
		},

		"Error: List Shards Failed (during pruning)": {
			tableGroup:      baseTG.DeepCopy(),
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnList: func(list client.ObjectList) error {
					if _, ok := list.(*multigresv1alpha1.ShardList); ok {
						return errSimulated
					}
					return nil
				},
			},
			expectedEvents: []string{"Warning CleanUpError Failed to list shards for pruning"},
		},
		"Error: Status List Failed (single child read)": {
			tableGroup:      baseTG.DeepCopy(),
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnList: func(list client.ObjectList) error {
					if _, ok := list.(*multigresv1alpha1.ShardList); ok {
						return errSimulated
					}
					return nil
				},
			},
			// Child Shards are read exactly once, so a List failure
			// surfaces as the pruning read error.
			expectedEvents: []string{"Warning CleanUpError Failed to list shards for pruning"},
		},
		"Error: Set PendingDeletion Failed": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{}
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(
					name.JoinWithConstraints(
						name.DefaultConstraints,
						clusterName,
						dbName,
						tgLabelName,
						"shard-0",
					),
					errSimulated,
				),
			},
			expectedEvents: []string{"Warning CleanUpError Failed to set PendingDeletion"},
		},
		"Error: Delete Orphan Shard Failed (After ReadyForDeletion)": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{}
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
						Annotations: map[string]string{
							multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
					Status: multigresv1alpha1.ShardStatus{
						Conditions: []metav1.Condition{
							{
								Type:               multigresv1alpha1.ConditionReadyForDeletion,
								Status:             metav1.ConditionTrue,
								Reason:             "DrainComplete",
								LastTransitionTime: metav1.Now(),
							},
						},
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnDelete: testutil.FailOnObjectName(
					name.JoinWithConstraints(
						name.DefaultConstraints,
						clusterName,
						dbName,
						tgLabelName,
						"shard-0",
					),
					errSimulated,
				),
			},
			expectedEvents: []string{"Warning CleanUpError Failed to delete orphan shard"},
		},
		"Error: Update Status Failed": {
			tableGroup:      baseTG.DeepCopy(),
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnStatusPatch: testutil.FailOnObjectName(tgName, errSimulated),
			},
			expectedEvents: []string{"Warning StatusError Failed to patch status"},
		},
		"Error: Handle Pending Deletion List Shards Failed": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				if tg.Annotations == nil {
					tg.Annotations = make(map[string]string)
				}
				tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = "2026-01-01T00:00:00Z"
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnList: func(list client.ObjectList) error {
					if _, ok := list.(*multigresv1alpha1.ShardList); ok {
						return errSimulated
					}
					return nil
				},
			},
		},
		"Error: Handle Pending Deletion Patch Shard Failed": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				if tg.Annotations == nil {
					tg.Annotations = make(map[string]string)
				}
				tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = "2026-01-01T00:00:00Z"
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(
					name.JoinWithConstraints(
						name.DefaultConstraints,
						clusterName,
						dbName,
						tgLabelName,
						"shard-0",
					),
					errSimulated,
				),
			},
		},
		"Error: Handle Pending Deletion Patch Status Failed": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(t testing.TB, tg *multigresv1alpha1.TableGroup) {
				if tg.Annotations == nil {
					tg.Annotations = make(map[string]string)
				}
				tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = "2026-01-01T00:00:00Z"
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name: name.JoinWithConstraints(
							name.DefaultConstraints,
							clusterName,
							dbName,
							tgLabelName,
							"shard-0",
						),
						Namespace: namespace,
						Labels: map[string]string{
							"multigres.com/cluster":    clusterName,
							"multigres.com/database":   dbName,
							"multigres.com/tablegroup": tgLabelName,
						},
						Annotations: map[string]string{
							multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
						},
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
					Status: multigresv1alpha1.ShardStatus{
						Conditions: []metav1.Condition{
							{
								Type:               multigresv1alpha1.ConditionReadyForDeletion,
								Status:             metav1.ConditionTrue,
								Reason:             "DrainComplete",
								LastTransitionTime: metav1.Now(),
							},
						},
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnStatusPatch: testutil.FailOnObjectName(tgName, errSimulated),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if tc.preReconcileUpdate != nil {
				tc.preReconcileUpdate(t, tc.tableGroup)
			}

			objects := tc.existingObjects
			// Create the TableGroup so reconcile reaches the failing step.
			objects = append(objects, tc.tableGroup)

			clientBuilder := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				WithStatusSubresource(&multigresv1alpha1.TableGroup{}, &multigresv1alpha1.Shard{})
			baseClient := clientBuilder.Build()

			finalClient := client.Client(baseClient)
			if tc.failureConfig != nil {
				finalClient = testutil.NewFakeClientWithFailures(baseClient, tc.failureConfig)
			}

			fakeRecorder := record.NewFakeRecorder(100)
			reconciler := &TableGroupReconciler{
				Client:   finalClient,
				Scheme:   scheme,
				Recorder: fakeRecorder,
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tc.tableGroup.Name,
					Namespace: tc.tableGroup.Namespace,
				},
			}

			_, err := reconciler.Reconcile(t.Context(), req)
			if err == nil {
				t.Error("Expected error from Reconcile, got nil")
			}

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
		})
	}
}

func TestTableGroupReconciler_Reconcile_BuildFailure(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	baseTG, _, _, _, _, _ := setupFixtures(t)

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseTG).Build()

	// The empty scheme makes BuildShard fail when it sets the owner reference.
	r := &TableGroupReconciler{
		Client:   c,
		Scheme:   runtime.NewScheme(),
		Recorder: record.NewFakeRecorder(100),
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: baseTG.Name, Namespace: baseTG.Namespace},
	}
	_, err := r.Reconcile(t.Context(), req)
	if err == nil {
		t.Fatal("Expected Reconcile to fail due to Build error")
	}
	if err.Error() != "failed to build shard: no kind is registered for the type v1alpha1.TableGroup" {
		t.Logf("Got error: %v", err)
	}
}

func TestSetupWithManager_Coverage(t *testing.T) {
	t.Parallel()

	t.Run("No Options", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Recovered expected panic: %v", r)
			}
		}()
		reconciler := &TableGroupReconciler{}
		_ = reconciler.SetupWithManager(nil)
	})

	t.Run("With Options", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Recovered expected panic: %v", r)
			}
		}()
		reconciler := &TableGroupReconciler{}
		_ = reconciler.SetupWithManager(nil, controller.Options{MaxConcurrentReconciles: 1})
	})
}

// TestTableGroupReconciler_ReadsChildShardsOnce asserts the reconcile lists
// child Shards exactly once. Listing them twice in a single pass (a List, then a
// re-List after writes) risks acting on a stale view, so an interceptor counts
// the *ShardList Lists and the test fails if there is more than one.
func TestTableGroupReconciler_ReadsChildShardsOnce(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	baseTG, tgName, namespace, clusterName, dbName, tgLabelName := setupFixtures(t)

	shardName := name.JoinWithConstraints(
		name.DefaultConstraints,
		clusterName,
		dbName,
		tgLabelName,
		"shard-0",
	)

	childShardLabels := map[string]string{
		"multigres.com/cluster":    clusterName,
		"multigres.com/database":   dbName,
		"multigres.com/tablegroup": tgLabelName,
	}

	tests := map[string]struct {
		tableGroup         *multigresv1alpha1.TableGroup
		preReconcileUpdate func(testing.TB, *multigresv1alpha1.TableGroup)
		existingObjects    []client.Object
	}{
		// Desired child already exists and is healthy.
		"steady state with healthy child shard": {
			tableGroup: baseTG.DeepCopy(),
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      shardName,
						Namespace: namespace,
						Labels:    childShardLabels,
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
					Status: multigresv1alpha1.ShardStatus{
						Phase: multigresv1alpha1.PhaseHealthy,
					},
				},
			},
		},
		// Spec wants no Shards, but an orphan child still exists to prune.
		"pending orphan child shard": {
			tableGroup: baseTG.DeepCopy(),
			preReconcileUpdate: func(_ testing.TB, tg *multigresv1alpha1.TableGroup) {
				tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{}
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Name:      shardName,
						Namespace: namespace,
						Labels:    childShardLabels,
					},
					Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
				},
			},
		},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			t.Parallel()

			if tc.preReconcileUpdate != nil {
				tc.preReconcileUpdate(t, tc.tableGroup)
			}

			objects := append([]client.Object{}, tc.existingObjects...)
			objects = append(objects, tc.tableGroup)

			// Count Lists of child Shards during one Reconcile.
			var shardListCount atomic.Int64

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				WithStatusSubresource(
					&multigresv1alpha1.TableGroup{},
					&multigresv1alpha1.Shard{},
				).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(
						ctx context.Context,
						cli client.WithWatch,
						list client.ObjectList,
						opts ...client.ListOption,
					) error {
						if _, ok := list.(*multigresv1alpha1.ShardList); ok {
							shardListCount.Add(1)
						}
						return cli.List(ctx, list, opts...)
					},
				}).
				Build()

			reconciler := &TableGroupReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(100),
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tgName,
					Namespace: namespace,
				},
			}

			if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("unexpected error from Reconcile: %v", err)
			}

			if got := shardListCount.Load(); got != 1 {
				t.Errorf(
					"expected exactly one List of child Shards per reconcile, got %d",
					got,
				)
			}
		})
	}
}

// TestTableGroupReconciler_PendingDeletionUpgradeCompatibility verifies that a
// child carrying a PendingDeletion annotation from an earlier operator version
// is handled correctly across an upgrade. The controller requeues until the
// child is ReadyForDeletion and then deletes it, without re-stamping the
// annotation or deleting before the child is ready.
func TestTableGroupReconciler_PendingDeletionUpgradeCompatibility(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	baseTG, tgName, namespace, clusterName, dbName, tgLabelName := setupFixtures(t)

	shardName := name.JoinWithConstraints(
		name.DefaultConstraints,
		clusterName,
		dbName,
		tgLabelName,
		"shard-0",
	)

	childShardLabels := map[string]string{
		"multigres.com/cluster":    clusterName,
		"multigres.com/database":   dbName,
		"multigres.com/tablegroup": tgLabelName,
	}

	// PendingDeletion value as if written by an earlier operator version; the
	// controller must keep it as-is.
	const priorVersionTimestamp = "2026-01-01T00:00:00Z"

	// orphanShard builds a no-longer-desired child already carrying the
	// PendingDeletion annotation. ready toggles the ReadyForDeletion condition.
	orphanShard := func(readyForDeletion bool) *multigresv1alpha1.Shard {
		s := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:      shardName,
				Namespace: namespace,
				Labels:    childShardLabels,
				Annotations: map[string]string{
					multigresv1alpha1.AnnotationPendingDeletion: priorVersionTimestamp,
				},
			},
			Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
		}
		if readyForDeletion {
			meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
				Type:    multigresv1alpha1.ConditionReadyForDeletion,
				Status:  metav1.ConditionTrue,
				Reason:  "Drained",
				Message: "All pods drained",
			})
		}
		return s
	}

	tests := map[string]struct {
		readyForDeletion bool
		wantResult       ctrl.Result
		wantDeleted      bool
	}{
		// It hasn't drained yet, so the controller should requeue and leave it alone.
		"annotation present but not ReadyForDeletion stays pending": {
			readyForDeletion: false,
			wantResult:       ctrl.Result{RequeueAfter: 5 * time.Second},
			wantDeleted:      false,
		},
		// Now that it's drained, the controller should delete it.
		"annotation present and ReadyForDeletion deletes the child": {
			readyForDeletion: true,
			wantResult:       ctrl.Result{},
			wantDeleted:      true,
		},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			t.Parallel()

			// No desired Shards, so the seeded child is an orphan.
			tg := baseTG.DeepCopy()
			tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{}

			child := orphanShard(tc.readyForDeletion)

			// Count child Patch/Delete calls to check the handshake isn't re-driven.
			var shardPatchCount atomic.Int64
			var shardDeleteCount atomic.Int64

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(child, tg).
				WithStatusSubresource(
					&multigresv1alpha1.TableGroup{},
					&multigresv1alpha1.Shard{},
				).
				WithInterceptorFuncs(interceptor.Funcs{
					Patch: func(
						ctx context.Context,
						cli client.WithWatch,
						obj client.Object,
						patch client.Patch,
						opts ...client.PatchOption,
					) error {
						if _, ok := obj.(*multigresv1alpha1.Shard); ok {
							shardPatchCount.Add(1)
						}
						return cli.Patch(ctx, obj, patch, opts...)
					},
					Delete: func(
						ctx context.Context,
						cli client.WithWatch,
						obj client.Object,
						opts ...client.DeleteOption,
					) error {
						if _, ok := obj.(*multigresv1alpha1.Shard); ok {
							shardDeleteCount.Add(1)
						}
						return cli.Delete(ctx, obj, opts...)
					},
				}).
				Build()

			reconciler := &TableGroupReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(100),
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tgName,
					Namespace: namespace,
				},
			}

			got, err := reconciler.Reconcile(t.Context(), req)
			if err != nil {
				t.Fatalf("unexpected error from Reconcile: %v", err)
			}

			if got != tc.wantResult {
				t.Errorf("unexpected reconcile result: got %+v, want %+v", got, tc.wantResult)
			}

			// The annotation must never be re-stamped.
			if patches := shardPatchCount.Load(); patches != 0 {
				t.Errorf(
					"handshake double-driven: expected zero Patch calls on the child Shard "+
						"(annotation already set by a prior version), got %d",
					patches,
				)
			}

			fetched := &multigresv1alpha1.Shard{}
			getErr := c.Get(
				t.Context(),
				types.NamespacedName{Name: shardName, Namespace: namespace},
				fetched,
			)

			if tc.wantDeleted {
				// Once it's drained the controller should delete it, exactly once.
				if !apierrors.IsNotFound(getErr) {
					t.Errorf(
						"expected orphan child Shard to be deleted once ReadyForDeletion, "+
							"but it still exists (get err: %v)",
						getErr,
					)
				}
				if deletes := shardDeleteCount.Load(); deletes != 1 {
					t.Errorf(
						"expected exactly one Delete of the child Shard, got %d",
						deletes,
					)
				}
				return
			}

			// While it's still draining the child should stay put and keep its
			// original annotation.
			if getErr != nil {
				t.Fatalf("expected orphan child Shard to still exist while draining: %v", getErr)
			}
			if deletes := shardDeleteCount.Load(); deletes != 0 {
				t.Errorf(
					"handshake double-driven: expected no Delete of the child Shard while it "+
						"is still draining, got %d",
					deletes,
				)
			}
			if got := fetched.Annotations[multigresv1alpha1.AnnotationPendingDeletion]; got !=
				priorVersionTimestamp {
				t.Errorf(
					"PendingDeletion annotation was re-stamped across the version boundary: "+
						"got %q, want preserved prior-version value %q",
					got,
					priorVersionTimestamp,
				)
			}
		})
	}
}

// TestTableGroupReconciler_PendingDeletionPublishesProgressingStatus verifies
// that a removed child still draining forces current Progressing status before
// the pending cleanup requeue.
func TestTableGroupReconciler_PendingDeletionPublishesProgressingStatus(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	baseTG, tgName, namespace, clusterName, dbName, tgLabelName := setupFixtures(t)

	shardName := name.JoinWithConstraints(
		name.DefaultConstraints,
		clusterName,
		dbName,
		tgLabelName,
		"shard-0",
	)

	tg := baseTG.DeepCopy()
	tg.Generation = 2
	tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{}
	tg.Status = multigresv1alpha1.TableGroupStatus{
		Phase:              multigresv1alpha1.PhaseHealthy,
		Message:            "Ready",
		TotalShards:        1,
		ReadyShards:        1,
		ObservedGeneration: 1,
	}
	meta.SetStatusCondition(&tg.Status.Conditions, metav1.Condition{
		Type:               "Available",
		Status:             metav1.ConditionTrue,
		Reason:             "ShardsReady",
		Message:            "1/1 shards ready",
		ObservedGeneration: 1,
	})

	orphan := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shardName,
			Namespace: namespace,
			Labels: map[string]string{
				"multigres.com/cluster":    clusterName,
				"multigres.com/database":   dbName,
				"multigres.com/tablegroup": tgLabelName,
			},
			Annotations: map[string]string{
				multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
		Status: multigresv1alpha1.ShardStatus{
			Phase:              multigresv1alpha1.PhaseHealthy,
			ObservedGeneration: 1,
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(orphan, tg).
		WithStatusSubresource(
			&multigresv1alpha1.TableGroup{},
			&multigresv1alpha1.Shard{},
		).
		Build()

	recorder := record.NewFakeRecorder(100)
	reconciler := &TableGroupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
	}

	got, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: tgName, Namespace: namespace},
	})
	if err != nil {
		t.Fatalf("unexpected error from Reconcile: %v", err)
	}
	if want := (ctrl.Result{RequeueAfter: 5 * time.Second}); got != want {
		t.Fatalf("unexpected reconcile result: got %+v, want %+v", got, want)
	}

	updatedTG := &multigresv1alpha1.TableGroup{}
	if err := c.Get(
		t.Context(),
		types.NamespacedName{Name: tgName, Namespace: namespace},
		updatedTG,
	); err != nil {
		t.Fatalf("failed to get tablegroup: %v", err)
	}

	if got, want := updatedTG.Status.Phase, multigresv1alpha1.PhaseProgressing; got != want {
		t.Errorf("Phase mismatch while cleanup is pending: got %q, want %q", got, want)
	}
	if got, want := updatedTG.Status.Message, "Waiting for removed shards to drain"; got != want {
		t.Errorf("Message mismatch while cleanup is pending: got %q, want %q", got, want)
	}
	if got, want := updatedTG.Status.ObservedGeneration, tg.Generation; got != want {
		t.Errorf("ObservedGeneration mismatch: got %d, want %d", got, want)
	}
	if got, want := updatedTG.Status.ReadyShards, int32(0); got != want {
		t.Errorf("ReadyShards mismatch: got %d, want %d", got, want)
	}
	if got, want := updatedTG.Status.TotalShards, int32(0); got != want {
		t.Errorf("TotalShards mismatch: got %d, want %d", got, want)
	}

	cond := meta.FindStatusCondition(updatedTG.Status.Conditions, "Available")
	if cond == nil {
		t.Fatal("expected an Available condition to be set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Available status mismatch: got %q, want %q", cond.Status, metav1.ConditionFalse)
	}
	if cond.Reason != "CleanupPending" {
		t.Errorf("Available reason mismatch: got %q, want CleanupPending", cond.Reason)
	}
	if cond.Message != "Waiting for removed shards to drain" {
		t.Errorf("Available message mismatch: got %q", cond.Message)
	}
	if cond.ObservedGeneration != tg.Generation {
		t.Errorf(
			"Available observedGeneration mismatch: got %d, want %d",
			cond.ObservedGeneration,
			tg.Generation,
		)
	}

	close(recorder.Events)
	for evt := range recorder.Events {
		if strings.Contains(evt, "Synced") {
			t.Errorf("unexpected Synced event while cleanup is still pending: %q", evt)
		}
	}
}

// TestTableGroupReconciler_SteadyStateDoesNotFlap verifies that reconciling an
// already-Healthy TableGroup does not emit a spurious PhaseChange event. The
// status-derivation half is covered by TestStepComputeStatus; this is the
// full-reconcile assertion, since the PhaseChange gating spans the whole chain.
func TestTableGroupReconciler_SteadyStateDoesNotFlap(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	baseTG, tgName, namespace, clusterName, dbName, tgLabelName := setupFixtures(t)

	shardName := name.JoinWithConstraints(
		name.DefaultConstraints,
		clusterName,
		dbName,
		tgLabelName,
		"shard-0",
	)

	childShardLabels := map[string]string{
		"multigres.com/cluster":    clusterName,
		"multigres.com/database":   dbName,
		"multigres.com/tablegroup": tgLabelName,
	}

	// Seed the TG already Healthy, so any flap would visibly change the phase.
	tg := baseTG.DeepCopy()
	tg.Status = multigresv1alpha1.TableGroupStatus{
		Phase:              multigresv1alpha1.PhaseHealthy,
		Message:            "Ready",
		TotalShards:        1,
		ReadyShards:        1,
		ObservedGeneration: tg.Generation,
	}

	// Healthy child with matching generations, so it counts as ready.
	childShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shardName,
			Namespace: namespace,
			Labels:    childShardLabels,
		},
		Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
		Status: multigresv1alpha1.ShardStatus{
			Phase: multigresv1alpha1.PhaseHealthy,
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(childShard, tg).
		WithStatusSubresource(
			&multigresv1alpha1.TableGroup{},
			&multigresv1alpha1.Shard{},
		).
		Build()

	recorder := record.NewFakeRecorder(100)

	reconciler := &TableGroupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      tgName,
			Namespace: namespace,
		},
	}

	got, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("unexpected error from Reconcile: %v", err)
	}

	// Nothing changed, so we shouldn't get a requeue.
	if got != (ctrl.Result{}) {
		t.Errorf("unexpected reconcile result: got %+v, want empty ctrl.Result{}", got)
	}

	// The status stays Healthy because it comes from the child's observed
	// .Status, not the empty-status desired object.
	updatedTG := &multigresv1alpha1.TableGroup{}
	if err := c.Get(
		t.Context(),
		types.NamespacedName{Name: tgName, Namespace: namespace},
		updatedTG,
	); err != nil {
		t.Fatalf("failed to get tablegroup: %v", err)
	}

	if got, want := updatedTG.Status.Phase, multigresv1alpha1.PhaseHealthy; got != want {
		t.Errorf(
			"steady-state reconcile flapped the phase: got %q, want %q (status must be "+
				"computed from observed children, not empty-status desired objects)",
			got,
			want,
		)
	}

	if got, want := updatedTG.Status.ReadyShards, int32(1); got != want {
		t.Errorf("ReadyShards mismatch: got %d, want %d", got, want)
	}

	// Phase didn't change, so there should be no PhaseChange event.
	close(recorder.Events)
	for evt := range recorder.Events {
		if strings.Contains(evt, "PhaseChange") {
			t.Errorf("unexpected spurious PhaseChange event emitted in steady state: %q", evt)
		}
	}
}
