//go:build integration
// +build integration

package shard

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// TestBackupStatus covers the backup verdict, which the shard controller
// writes apart from its other status, against a real API server.
func TestBackupStatus(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))
	cfg := testutil.SetUpEnvtest(
		t,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	c := testutil.SetUpClient(t, cfg, scheme)
	r := &ShardReconciler{
		Client:   ownership.SkipNoopApplies(c),
		Recorder: record.NewFakeRecorder(100),
	}

	newShard := func(t *testing.T, name string) func() *multigresv1alpha1.Shard {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "postgres",
				TableGroupName: "default",
				ShardName:      "0-inf",
				PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
					Name: "multigres-admin-password",
					Key:  PostgresPasswordSecretKey,
				},
				Images: multigresv1alpha1.ShardImages{
					Multiorch:   "ghcr.io/multigres/multigres:main",
					Multipooler: "ghcr.io/multigres/multigres:main",
					Postgres:    "postgres:17",
				},
				GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
					Address:        "global-topo:2379",
					RootPath:       "/multigres/global",
					Implementation: "etcd",
				},
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {
						Cells:   []multigresv1alpha1.CellName{"zone-a"},
						Type:    "readWrite",
						Storage: multigresv1alpha1.StorageSpec{Size: "1Gi"},
					},
				},
			},
		}
		require.NoError(t, c.Create(t.Context(), shard))
		return func() *multigresv1alpha1.Shard {
			s := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(shard), s))
			return s
		}
	}
	verdict := func(s *multigresv1alpha1.Shard) {
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
			Type:               backuphealth.ConditionHealthy,
			Status:             metav1.ConditionTrue,
			Reason:             "BackupRecent",
			Message:            "recent backup",
			ObservedGeneration: s.Generation,
		})
	}
	editSpec := func(t *testing.T, get func() *multigresv1alpha1.Shard, image string) *multigresv1alpha1.Shard {
		s := get()
		s.Spec.Images.Postgres = multigresv1alpha1.ImageRef(image)
		require.NoError(t, c.Update(t.Context(), s))
		return get()
	}
	healthyGeneration := func(t *testing.T, s *multigresv1alpha1.Shard) int64 {
		cond := meta.FindStatusCondition(s.Status.Conditions, backuphealth.ConditionHealthy)
		require.NotNil(t, cond)
		return cond.ObservedGeneration
	}

	// A pass starting from a cached Shard older than the verdict the previous
	// pass wrote must not put the older verdict back: were it to, the next
	// pass would write the newer one again, and BackupHealthy's
	// observedGeneration would alternate between two generations for as long
	// as the cache lagged.
	t.Run("a stale snapshot keeps the newer verdict", func(t *testing.T) {
		ctx := t.Context()
		get := newShard(t, "stale-after")

		s := get()
		s.Status.Phase = multigresv1alpha1.PhaseProgressing
		s.Status.ObservedGeneration = s.Generation
		s.Status.OrchReady = true
		verdict(s)
		require.NoError(t, r.patchStatus(ctx, s, ""))
		require.NoError(t, r.applyBackupStatus(ctx, s, true))

		got := get()
		require.True(t, got.Status.OrchReady)
		require.False(t, ownership.Owns(got, ownership.ManagerShardBackup, ".status.orchReady"))

		stale := get()
		s = editSpec(t, get, "postgres:18")
		require.Equal(t, int64(2), s.Generation)
		verdict(s)
		require.NoError(t, r.applyBackupStatus(ctx, s, true))

		stale.Status.Phase = multigresv1alpha1.PhaseHealthy
		require.NoError(t, r.patchStatus(ctx, stale, multigresv1alpha1.PhaseProgressing))

		got = get()
		require.Equal(t, multigresv1alpha1.PhaseHealthy, got.Status.Phase)
		require.Equal(t, int64(2), healthyGeneration(t, got))
		require.True(t, got.Status.OrchReady)
	})

	// A Shard an earlier operator wrote holds its verdict under the main
	// status manager. A snapshot taken before this operator's backup manager
	// first writes the verdict must not put the earlier verdict back over it.
	t.Run("a snapshot from before the handoff keeps the newer verdict", func(t *testing.T) {
		ctx := t.Context()
		get := newShard(t, "stale-before")

		old := get()
		old.Status.Phase = multigresv1alpha1.PhaseHealthy
		old.Status.ObservedGeneration = old.Generation
		old.Status.LastBackupTime = &metav1.Time{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
		old.Status.LastBackupType = "full"
		verdict(old)
		require.NoError(t, ownership.ApplyStatus(ctx, c, &multigresv1alpha1.Shard{
			TypeMeta: metav1.TypeMeta{
				APIVersion: multigresv1alpha1.GroupVersion.String(),
				Kind:       "Shard",
			},
			ObjectMeta: metav1.ObjectMeta{Name: old.Name, Namespace: old.Namespace},
			Status:     old.Status,
		}, ownership.ManagerShard))

		stale := get()
		require.Equal(t, int64(1), healthyGeneration(t, stale))

		s := editSpec(t, get, "postgres:18")
		s.Status.ObservedGeneration = s.Generation
		verdict(s)
		require.NoError(t, r.patchStatus(ctx, s, multigresv1alpha1.PhaseHealthy))
		require.NoError(t, r.applyBackupStatus(ctx, s, true))
		require.Equal(t, int64(2), healthyGeneration(t, get()))

		err := r.patchStatus(ctx, stale, multigresv1alpha1.PhaseHealthy)
		require.True(
			t,
			apierrors.IsConflict(err),
			"adopting from a stale snapshot is refused: %v",
			err,
		)

		got := get()
		require.Equal(t, int64(2), healthyGeneration(t, got))
		require.NotNil(t, got.Status.LastBackupTime)
		require.Equal(t, "full", got.Status.LastBackupType)
	})

	// A new Shard whose pooler client cannot be built yet gets its status
	// and its verdict: the verdict alone would be refused, the status's
	// required fields being unset, so it is written after the status.
	t.Run("a new Shard without a pooler client gets its verdict", func(t *testing.T) {
		ctx := t.Context()
		get := newShard(t, "no-client")
		store, _ := postureTestStore(t)
		rr := &ShardReconciler{
			Client:   ownership.SkipNoopApplies(c),
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(100),
			PoolerClients: &countingPoolerResolver{
				client: rpcclient.NewFakeClient(),
				err:    errors.New("certificate not issued"),
			},
			CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
				return store, nil
			},
		}
		_, err := rr.reconcileDataPlane(ctx, get(), renderedConfig{}, &shardRolloutTracker{})
		require.NoError(t, err)
		cond := meta.FindStatusCondition(get().Status.Conditions, backuphealth.ConditionHealthy)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionUnknown, cond.Status)
	})

	// An operator that wrote the verdict and the last backup with the rest of
	// the status left them under the main status manager. The first pass of
	// this operator hands them to the backup managers before its status apply
	// leaves them out, so a pass that cannot evaluate backups keeps them.
	t.Run("an upgrade keeps the legacy backup status", func(t *testing.T) {
		lastBackup := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		legacy := func(t *testing.T, name string) func() *multigresv1alpha1.Shard {
			get := newShard(t, name)
			old := get()
			old.Status.Phase = multigresv1alpha1.PhaseHealthy
			old.Status.ObservedGeneration = old.Generation
			old.Status.LastBackupTime = &lastBackup
			old.Status.LastBackupType = "full"
			verdict(old)
			require.NoError(t, ownership.ApplyStatus(t.Context(), c, &multigresv1alpha1.Shard{
				TypeMeta: metav1.TypeMeta{
					APIVersion: multigresv1alpha1.GroupVersion.String(),
					Kind:       "Shard",
				},
				ObjectMeta: metav1.ObjectMeta{Name: old.Name, Namespace: old.Namespace},
				Status:     old.Status,
			}, ownership.ManagerShard))
			return get
		}
		reconciler := func(t *testing.T, clients poolerclient.Resolver) *ShardReconciler {
			// A topology with the Shard's cell and no poolers, so no
			// primary exists to list backups from.
			_, factory := memorytopo.NewServerAndFactory(t.Context(), "zone-a")
			store := topoclient.NewWithFactory(
				factory,
				"",
				[]string{""},
				topoclient.NewDefaultTopoConfig(),
			)
			t.Cleanup(func() { _ = store.Close() })
			return &ShardReconciler{
				Client:        ownership.SkipNoopApplies(c),
				Scheme:        scheme,
				Recorder:      record.NewFakeRecorder(100),
				PoolerClients: clients,
				CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
					return store, nil
				},
			}
		}
		handedOver := func(t *testing.T, got *multigresv1alpha1.Shard) {
			t.Helper()
			require.NotNil(t, got.Status.LastBackupTime)
			require.True(t, lastBackup.Equal(got.Status.LastBackupTime))
			require.Equal(t, "full", got.Status.LastBackupType)
			require.True(
				t,
				ownership.Owns(got, ownership.ManagerShardLastBackup, ".status.lastBackupTime"),
			)
			require.True(t, ownership.Owns(got, ownership.ManagerShardBackup,
				ownership.ConditionPath(backuphealth.ConditionHealthy)))
			require.False(t, ownership.Owns(got, ownership.ManagerShard,
				ownership.ConditionPath(backuphealth.ConditionHealthy)))
			require.False(t, ownership.Owns(got, ownership.ManagerShard, ".status.lastBackupTime"))
		}

		t.Run("without a pooler client", func(t *testing.T) {
			get := legacy(t, "upgrade-no-client")
			rr := reconciler(t, &countingPoolerResolver{
				client: rpcclient.NewFakeClient(),
				err:    errors.New("certificate not issued"),
			})
			_, err := rr.reconcileDataPlane(t.Context(), get(), renderedConfig{}, &shardRolloutTracker{})
			require.NoError(t, err)
			got := get()
			handedOver(t, got)
			cond := meta.FindStatusCondition(got.Status.Conditions, backuphealth.ConditionHealthy)
			require.NotNil(t, cond)
			require.Equal(
				t,
				metav1.ConditionUnknown,
				cond.Status,
				"an unchecked verdict is Unknown",
			)
		})

		t.Run("without a primary to list backups from", func(t *testing.T) {
			get := legacy(t, "upgrade-no-primary")
			rr := reconciler(t, poolerclient.Static(rpcclient.NewFakeClient()))
			_, err := rr.reconcileDataPlane(t.Context(), get(), renderedConfig{}, &shardRolloutTracker{})
			require.NoError(t, err)
			got := get()
			handedOver(t, got)
			cond := meta.FindStatusCondition(got.Status.Conditions, backuphealth.ConditionHealthy)
			require.NotNil(t, cond)
			require.Equal(
				t,
				metav1.ConditionTrue,
				cond.Status,
				"nothing was evaluated, so the verdict stands",
			)
		})

		// A whole pass writes status twice, once early and once after the
		// data-plane observation, from the one Shard it read. The handoff on
		// the first write must leave the second nothing to hand off, or the
		// second would repeat it on the snapshot the first made stale, fail
		// with a conflict and stop the pass.
		t.Run("through a whole pass", func(t *testing.T) {
			ctx := t.Context()
			get := legacy(t, "upgrade-whole-pass")
			s := get()
			s.Finalizers = append(s.Finalizers, shardFinalizer)
			require.NoError(t, c.Update(ctx, s))
			require.NoError(t, client.IgnoreAlreadyExists(c.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multigres-admin-password",
					Namespace: "default",
				},
				Data: map[string][]byte{PostgresPasswordSecretKey: []byte("postgres")},
			})))
			rr := reconciler(t, poolerclient.Static(rpcclient.NewFakeClient()))
			recorder := rr.Recorder.(*record.FakeRecorder)
			_, err := rr.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(s)})
			require.NoError(t, err)
			close(recorder.Events)
			for e := range recorder.Events {
				// envtest runs no pods, so the pool is empty; any other
				// warning is a failure of the pass.
				if !strings.Contains(e, "Warning PoolEmpty") {
					require.NotContains(t, e, "Warning", "the upgrade pass records no failure")
				}
			}
			got := get()
			handedOver(t, got)
			cond := meta.FindStatusCondition(got.Status.Conditions, backuphealth.ConditionHealthy)
			require.NotNil(t, cond)
			require.Equal(t, metav1.ConditionTrue, cond.Status)
		})
	})

	// A pass that could not list backups observed no last backup, so its
	// verdict must leave the last backup's time and type as the last listing
	// wrote them, whatever its own cached copy holds.
	t.Run("a failed check keeps the last backup", func(t *testing.T) {
		ctx := t.Context()
		get := newShard(t, "failed-check")

		s := get()
		s.Status.Phase = multigresv1alpha1.PhaseHealthy
		require.NoError(t, r.patchStatus(ctx, s, ""))
		s = get()
		older := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		s.Status.LastBackupTime = &older
		s.Status.LastBackupType = "full"
		verdict(s)
		require.NoError(t, r.applyBackupStatus(ctx, s, true))
		stale := get()
		s = get()
		latest := metav1.NewTime(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC))
		s.Status.LastBackupTime = &latest
		s.Status.LastBackupType = "incr"
		verdict(s)
		require.NoError(t, r.applyBackupStatus(ctx, s, true))

		meta.SetStatusCondition(&stale.Status.Conditions, metav1.Condition{
			Type:    backuphealth.ConditionHealthy,
			Status:  metav1.ConditionUnknown,
			Reason:  reasonBackupCheckUnavailable,
			Message: "listing failed",
		})
		require.NoError(t, r.applyBackupStatus(ctx, stale, false))

		got := get()
		cond := meta.FindStatusCondition(got.Status.Conditions, backuphealth.ConditionHealthy)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionUnknown, cond.Status)
		require.NotNil(t, got.Status.LastBackupTime)
		require.True(t, latest.Equal(got.Status.LastBackupTime))
		require.Equal(t, "incr", got.Status.LastBackupType)
	})
}
