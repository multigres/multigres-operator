//go:build integration
// +build integration

package tablegroup_test

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/testutil"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
)

func TestTableGroup_Lifecycle(t *testing.T) {
	t.Parallel()

	globalTopo := multigresv1alpha1.GlobalTopoServerRef{
		Address:        "etcd-client:2379",
		RootPath:       "/multigres/global",
		Implementation: "etcd",
	}

	setup := func(t *testing.T) (client.Client, *testutil.ResourceWatcher) {
		t.Helper()

		// These tests need the real apiserver/cache behaviour from envtest because
		// the lifecycle under test depends on watches, status updates, and
		// optimistic concurrency rather than just object shape.
		scheme := runtime.NewScheme()
		_ = multigresv1alpha1.AddToScheme(scheme)
		_ = appsv1.AddToScheme(scheme)
		_ = corev1.AddToScheme(scheme)

		mgr := testutil.SetUpEnvtestManager(t, scheme,
			testutil.WithCRDPaths("../../../../config/crd/bases"),
		)

		if err := (&tablegroup.TableGroupReconciler{
			Client:   mgr.GetClient(),
			Scheme:   mgr.GetScheme(),
			Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
		}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}); err != nil {
			t.Fatal(err)
		}

		watcher := testutil.NewResourceWatcher(t, t.Context(), mgr,
			testutil.WithCmpOpts(testutil.IgnoreMetaRuntimeFields()),
			testutil.WithExtraResource(&multigresv1alpha1.Shard{}),
			testutil.WithTimeout(10*time.Second),
		)
		return mgr.GetClient(), watcher
	}

	t.Run("Pruning (Orphan Deletion)", func(t *testing.T) {
		t.Parallel()
		k8sClient, watcher := setup(t)
		ctx := t.Context()

		tgName := "tg-prune"
		clusterName := "prune-cluster"
		tg := &multigresv1alpha1.TableGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tgName,
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": clusterName},
			},
			Spec: multigresv1alpha1.TableGroupSpec{
				DatabaseName: "db1", TableGroupName: "tg1",
				GlobalTopoServer: globalTopo,
				Images: multigresv1alpha1.ShardImages{
					MultiOrch:   "orch:latest",
					MultiPooler: "pooler:latest",
					Postgres:    "postgres:15",
				},
				Shards: []multigresv1alpha1.ShardResolvedSpec{
					{
						Name: "keep-me",
						MultiOrch: multigresv1alpha1.MultiOrchSpec{
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Replicas: ptr.To(int32(1)),
							},
						},
						Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
					},
					{
						Name: "delete-me",
						MultiOrch: multigresv1alpha1.MultiOrchSpec{
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Replicas: ptr.To(int32(1)),
							},
						},
						Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
					},
				},
			},
		}

		// Begin with two desired children so removing one from the spec later has
		// to go through the same graceful-deletion path used in production.
		setTestPostgresPasswordSecretRef(tg)
		if err := k8sClient.Create(ctx, tg); err != nil {
			t.Fatal(err)
		}

		// The expected Shards describe the child specs TableGroup owns. Metadata
		// and status are intentionally ignored because those are filled by the
		// apiserver and by child controllers.
		shard1 := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: nameutil.JoinWithConstraints(
					nameutil.DefaultConstraints,
					clusterName,
					"db1",
					"tg1",
					"keep-me",
				),
				Namespace: "default",
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:     "db1",
				TableGroupName:   "tg1",
				ShardName:        "keep-me",
				GlobalTopoServer: globalTopo,
				Images: multigresv1alpha1.ShardImages{
					MultiOrch:   "orch:latest",
					MultiPooler: "pooler:latest",
					Postgres:    "postgres:15",
				},
				MultiOrch: multigresv1alpha1.MultiOrchSpec{
					StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
				},
				Pools:    map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
				Replicas: ptr.To(int32(0)),
			},
		}
		shard2 := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: nameutil.JoinWithConstraints(
					nameutil.DefaultConstraints,
					clusterName,
					"db1",
					"tg1",
					"delete-me",
				),
				Namespace: "default",
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:     "db1",
				TableGroupName:   "tg1",
				ShardName:        "delete-me",
				GlobalTopoServer: globalTopo,
				Images: multigresv1alpha1.ShardImages{
					MultiOrch:   "orch:latest",
					MultiPooler: "pooler:latest",
					Postgres:    "postgres:15",
				},
				MultiOrch: multigresv1alpha1.MultiOrchSpec{
					StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
				},
				Pools:    map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
				Replicas: ptr.To(int32(0)),
			},
		}
		setTestShardPostgresPasswordSecretRef(shard1)
		setTestShardPostgresPasswordSecretRef(shard2)

		// Compare only spec fields because the controller owns metadata and status.
		watcher.SetCmpOpts(testutil.CompareSpecOnly()...)
		if err := watcher.WaitForMatch(shard1, shard2); err != nil {
			t.Fatalf("Failed to create initial shards: %v", err)
		}

		// Removing a child from the desired spec should not delete the Shard
		// immediately. The parent first asks the child to drain by preserving the
		// object and adding PendingDeletion.
		// Retry because the controller may update status or finalizers concurrently.
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tg), tg); err != nil {
				return err
			}
			tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{
				{
					Name: "keep-me",
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
				},
			}
			return k8sClient.Update(ctx, tg)
		}); err != nil {
			t.Fatal(err)
		}

		// The shard controller is not running in this test. Once the parent has
		// asked for a drain, the test simulates the child controller's
		// ReadyForDeletion acknowledgement.
		deleteMeName := nameutil.JoinWithConstraints(
			nameutil.DefaultConstraints,
			clusterName,
			"db1",
			"tg1",
			"delete-me",
		)
		var deleteMe multigresv1alpha1.Shard
		waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
		defer waitCancel()
		for {
			if err := k8sClient.Get(
				waitCtx,
				client.ObjectKey{Name: deleteMeName, Namespace: "default"},
				&deleteMe,
			); err == nil {
				if deleteMe.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != "" {
					break
				}
			}
			select {
			case <-waitCtx.Done():
				t.Fatalf("Shard 'delete-me' was not marked PendingDeletion: %v", waitCtx.Err())
			case <-time.After(200 * time.Millisecond):
			}
		}

		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &multigresv1alpha1.Shard{}
			if err := k8sClient.Get(
				ctx,
				client.ObjectKey{Name: deleteMeName, Namespace: "default"},
				latest,
			); err != nil {
				return err
			}
			latest.Status.Conditions = append(latest.Status.Conditions, metav1.Condition{
				Type:               multigresv1alpha1.ConditionReadyForDeletion,
				Status:             metav1.ConditionTrue,
				Reason:             "DrainComplete",
				Message:            "Simulated by integration test",
				LastTransitionTime: metav1.Now(),
			})
			return k8sClient.Status().Update(ctx, latest)
		}); err != nil {
			t.Fatalf("Failed to set ReadyForDeletion condition: %v", err)
		}

		// Deletion is only valid after the child has acknowledged the drain.
		if err := watcher.WaitForDeletion(shard2); err != nil {
			t.Errorf("Shard 'delete-me' was not pruned: %v", err)
		}
	})

	t.Run("Enforcement (Revert Manual Changes)", func(t *testing.T) {
		t.Parallel()
		k8sClient, watcher := setup(t)
		ctx := t.Context()

		tgName := "tg-enforce"
		clusterName := "enforce-cluster"
		tg := &multigresv1alpha1.TableGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tgName,
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": clusterName},
			},
			Spec: multigresv1alpha1.TableGroupSpec{
				DatabaseName:     "db1",
				TableGroupName:   "tg1",
				GlobalTopoServer: globalTopo,
				Images: multigresv1alpha1.ShardImages{
					MultiOrch:   "orch:latest",
					MultiPooler: "pooler:latest",
					Postgres:    "postgres:15",
				},
				Shards: []multigresv1alpha1.ShardResolvedSpec{
					{
						Name: "s1",
						MultiOrch: multigresv1alpha1.MultiOrchSpec{
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Replicas: ptr.To(int32(1)),
							},
						},
						Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
					},
				},
			},
		}

		// This case treats TableGroup as the source of truth for child spec. A
		// direct edit to the Shard should be corrected back to the parent-owned
		// desired shape.
		setTestPostgresPasswordSecretRef(tg)

		if err := k8sClient.Create(ctx, tg); err != nil {
			t.Fatal(err)
		}

		// Compare only spec fields because the controller owns metadata and status.
		watcher.SetCmpOpts(testutil.CompareSpecOnly()...)

		goodShard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: nameutil.JoinWithConstraints(
					nameutil.DefaultConstraints,
					clusterName,
					"db1",
					"tg1",
					"s1",
				),
				Namespace: "default",
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:     "db1",
				TableGroupName:   "tg1",
				ShardName:        "s1",
				GlobalTopoServer: globalTopo,
				Images: multigresv1alpha1.ShardImages{
					MultiOrch:   "orch:latest",
					MultiPooler: "pooler:latest",
					Postgres:    "postgres:15",
				},
				MultiOrch: multigresv1alpha1.MultiOrchSpec{
					StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
				},
				Pools:    map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
				Replicas: ptr.To(int32(0)),
			},
		}
		setTestShardPostgresPasswordSecretRef(goodShard)
		if err := watcher.WaitForMatch(goodShard); err != nil {
			t.Fatalf("Initial shard creation failed: %v", err)
		}

		// Mutate the child directly, as an out-of-band actor might. The retry is
		// for resourceVersion conflicts with the controller's own writes.
		latestShard := &multigresv1alpha1.Shard{}
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := k8sClient.Get(
				ctx,
				client.ObjectKeyFromObject(goodShard),
				latestShard,
			); err != nil {
				return err
			}
			latestShard.Spec.MultiOrch.Replicas = ptr.To(int32(99))
			return k8sClient.Update(ctx, latestShard)
		}); err != nil {
			t.Fatal(err)
		}

		// Assert the desired spec again rather than trying to observe the bad
		// intermediate state; the controller may repair it before the watch sees it.
		if err := watcher.WaitForMatch(goodShard); err != nil {
			t.Errorf("Controller failed to revert manual shard change: %v", err)
		}
	})
}
