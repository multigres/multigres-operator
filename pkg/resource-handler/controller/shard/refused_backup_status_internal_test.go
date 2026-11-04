package shard

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

type failingPoolerResolver struct{ err error }

func (f failingPoolerResolver) ClientFor(
	context.Context, *multigresv1alpha1.Shard,
) (rpcclient.MultipoolerClient, error) {
	return nil, f.err
}

// A refused shard's pods keep running and keep taking backups, or stop. The
// BackupHealthy condition a reader sees must follow them: a verdict from
// before the refusal that said True is replaced by what the running pods
// report, or by Unknown when they cannot be asked. The last backup's time
// and type stay as the last successful listing wrote them.
func TestRefusedShardDoesNotKeepAStaleBackupVerdict(t *testing.T) {
	lastBackup := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	for name, tc := range map[string]struct {
		clients func(*rpcclient.FakeClient) poolerclient.Resolver
		backups []*multipoolermanagerdatapb.BackupMetadata
		want    metav1.ConditionStatus
		// observed is whether the pass listed backups, and so wrote the last
		// backup's time.
		observed bool
		// What the pass has to look with: no registered primary, no topology
		// store, no pool pod, no pooler client factory, a Lite shard.
		noPrimary, noStore, noPods, noClients, lite bool
	}{
		"no primary is registered": {
			clients: func(rpc *rpcclient.FakeClient) poolerclient.Resolver {
				return poolerclient.Static(rpc)
			},
			noPrimary: true,
			want:      metav1.ConditionUnknown,
		},
		"the topology store cannot be opened": {
			clients: func(rpc *rpcclient.FakeClient) poolerclient.Resolver {
				return poolerclient.Static(rpc)
			},
			noStore: true,
			want:    metav1.ConditionUnknown,
		},
		"the shard has no pool pod": {
			clients: func(rpc *rpcclient.FakeClient) poolerclient.Resolver {
				return poolerclient.Static(rpc)
			},
			noPods: true,
			want:   metav1.ConditionUnknown,
		},
		"no pooler client is configured": {
			noClients: true,
			want:      metav1.ConditionUnknown,
		},
		"a Lite shard": {
			clients: func(rpc *rpcclient.FakeClient) poolerclient.Resolver {
				return poolerclient.Static(rpc)
			},
			lite: true,
			want: metav1.ConditionUnknown,
		},
		"the backups cannot be listed": {
			clients: func(rpc *rpcclient.FakeClient) poolerclient.Resolver {
				return poolerclient.Static(backupErrorClient{
					MultipoolerClient: rpc, err: errors.New("backup RPC unavailable"),
				})
			},
			want: metav1.ConditionUnknown,
		},
		"the poolers cannot be reached": {
			clients: func(*rpcclient.FakeClient) poolerclient.Resolver {
				return failingPoolerResolver{errors.New("no client certificate")}
			},
			want: metav1.ConditionUnknown,
		},
		"the pods report no backup": {
			clients: func(rpc *rpcclient.FakeClient) poolerclient.Resolver {
				return poolerclient.Static(rpc)
			},
			want:     metav1.ConditionFalse,
			observed: true,
		},
		"the pods report a recent backup": {
			clients: func(rpc *rpcclient.FakeClient) poolerclient.Resolver {
				return poolerclient.Static(rpc)
			},
			backups: []*multipoolermanagerdatapb.BackupMetadata{{
				Type: "full", Status: multipoolermanagerdatapb.BackupMetadata_COMPLETE,
				StopTimestamp: timestamppb.New(time.Now().Add(-time.Hour)),
			}},
			want:     metav1.ConditionTrue,
			observed: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			shard := postureTestShard()
			if tc.lite {
				shard.Spec.Mode = multigresv1alpha1.ModeLite
			}
			store, poolerID := postureTestStore(t)
			if !tc.noPrimary {
				_, err := store.UpdateMultipoolerFields(ctx,
					&clustermetadata.ID{Cell: "cell1", Name: "pooler-0"},
					func(pooler *clustermetadata.Multipooler) error {
						pooler.RoutingState.Role = clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY
						return nil
					})
				if err != nil {
					t.Fatal(err)
				}
			}
			rpc := rpcclient.NewFakeClient()
			rpc.SetStatusResponse(poolerID, &multipoolermanagerdatapb.StatusResponse{
				Status: &multipoolermanagerdatapb.Status{
					PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY,
				},
			})
			rpc.GetBackupsResponses[poolerID] = &multipoolermanagerdatapb.GetBackupsResponse{
				Backups: tc.backups,
			}
			scheme := postureTestScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithTypeConverters(testutil.TypeConverters(t, "../../../../config/crd/bases")...).
				WithObjects(shard, postureTestPod()).WithStatusSubresource(shard).
				WithReturnManagedFields().Build()
			recorder := record.NewFakeRecorder(20)
			r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: recorder}
			if !tc.noClients {
				r.PoolerClients = tc.clients(rpc)
			}
			t.Cleanup(func() { _ = store.Close() })
			r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
				return keepOpenTopo{store}, nil
			}

			// The verdict and the last backup from before the refusal, stored
			// the way an accepted pass stores them.
			if err := r.updateStatus(ctx, shard, renderedConfig{}); err != nil {
				t.Fatal(err)
			}
			shard.Status.LastBackupTime = &lastBackup
			shard.Status.LastBackupType = "full"
			meta.SetStatusCondition(&shard.Status.Conditions, metav1.Condition{
				Type: backuphealth.ConditionHealthy, Status: metav1.ConditionTrue,
				Reason: "BackupRecent", Message: "Last backup completed before the refusal",
			})
			if err := r.applyBackupStatus(ctx, shard, true); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(shard), shard); err != nil {
				t.Fatal(err)
			}

			// What the pass cannot look with is taken away after the verdict
			// was stored: the refused shard had all of it before.
			if tc.noPods {
				if err := c.Delete(ctx, postureTestPod()); err != nil {
					t.Fatal(err)
				}
			}
			if tc.noStore {
				r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
					return nil, errors.New("etcd is unreachable")
				}
			}
			if _, err := r.observeRefusedShard(ctx, shard, renderedConfig{}); err != nil {
				t.Fatal(err)
			}

			// A second pass that finds the same leaves the verdict as it is
			// and does not report the failure again: a refused shard is
			// observed on a requeue for as long as it is refused.
			if tc.want == metav1.ConditionUnknown {
				stored := &multigresv1alpha1.Shard{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(shard), stored); err != nil {
					t.Fatal(err)
				}
				for len(recorder.Events) > 0 {
					<-recorder.Events
				}
				if _, err := r.observeRefusedShard(ctx, stored, renderedConfig{}); err != nil {
					t.Fatal(err)
				}
				again := &multigresv1alpha1.Shard{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(shard), again); err != nil {
					t.Fatal(err)
				}
				was := meta.FindStatusCondition(
					stored.Status.Conditions,
					backuphealth.ConditionHealthy,
				)
				is := meta.FindStatusCondition(
					again.Status.Conditions,
					backuphealth.ConditionHealthy,
				)
				if was == nil || is == nil || *was != *is {
					t.Errorf(
						"a pass that found the same changed the verdict: %+v, was %+v",
						is,
						was,
					)
				}
				for len(recorder.Events) > 0 {
					if e := <-recorder.Events; strings.Contains(e, "BackupCheckFailed") {
						t.Errorf("a pass that found the same reported again: %s", e)
					}
				}
			}

			got := &multigresv1alpha1.Shard{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(shard), got); err != nil {
				t.Fatal(err)
			}
			verdict := meta.FindStatusCondition(
				got.Status.Conditions,
				backuphealth.ConditionHealthy,
			)
			if verdict == nil || verdict.Status != tc.want {
				t.Errorf("BackupHealthy = %+v, want %s", verdict, tc.want)
			}
			if tc.observed {
				return
			}
			if got.Status.LastBackupTime == nil || !got.Status.LastBackupTime.Equal(&lastBackup) ||
				got.Status.LastBackupType != "full" {
				t.Errorf("last backup = %v %q, want the one from the last listing kept",
					got.Status.LastBackupTime, got.Status.LastBackupType)
			}
		})
	}
}

// A shard whose last listing found no usable backup has BackupHealthy=False,
// and a listing that then fails leaves that verdict as it is. The failure is
// reported with one event, not one per pass: the shard is reconciled on a
// requeue for as long as the primary cannot list. Another failure, and the
// same one after a listing that worked, are reported again.
func TestBackupCheckFailureIsReportedOnceWhileTheVerdictIsFalse(t *testing.T) {
	ctx := t.Context()
	shard := postureTestShard()
	store, poolerID := postureTestStore(t)
	t.Cleanup(func() { _ = store.Close() })
	_, err := store.UpdateMultipoolerFields(ctx,
		&clustermetadata.ID{Cell: "cell1", Name: "pooler-0"},
		func(pooler *clustermetadata.Multipooler) error {
			pooler.RoutingState.Role = clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY
			return nil
		})
	require.NoError(t, err)
	rpc := rpcclient.NewFakeClient()
	rpc.GetBackupsResponses[poolerID] = &multipoolermanagerdatapb.GetBackupsResponse{}
	scheme := postureTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithTypeConverters(testutil.TypeConverters(t, "../../../../config/crd/bases")...).
		WithObjects(shard, postureTestPod()).WithStatusSubresource(shard).
		WithReturnManagedFields().Build()
	recorder := record.NewFakeRecorder(50)
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: recorder}
	failures := func() (n int) {
		for len(recorder.Events) > 0 {
			if e := <-recorder.Events; strings.Contains(e, "BackupCheckFailed") {
				n++
			}
		}
		return n
	}
	verdict := func() metav1.Condition {
		t.Helper()
		got := &multigresv1alpha1.Shard{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
		cond := meta.FindStatusCondition(got.Status.Conditions, backuphealth.ConditionHealthy)
		require.NotNil(t, cond)
		return *cond
	}
	shardKeyOf := client.ObjectKeyFromObject(shard)
	pass := func(rpcClient rpcclient.MultipoolerClient) {
		t.Helper()
		got := &multigresv1alpha1.Shard{}
		require.NoError(t, c.Get(ctx, shardKeyOf, got))
		asked, err := r.reconcileBackupHealth(ctx, store, rpcClient, got)
		require.NoError(t, err)
		require.True(t, asked)
	}

	// The listing works and finds no backup.
	pass(rpc)
	unhealthy := verdict()
	require.Equal(t, metav1.ConditionFalse, unhealthy.Status)
	require.Zero(t, failures())

	failing := backupErrorClient{MultipoolerClient: rpc, err: errors.New("backup RPC unavailable")}
	for i := range 5 {
		pass(failing)
		require.Equal(t, unhealthy, verdict(), "the failed listing changed the verdict")
		if i == 0 {
			require.Equal(t, 1, failures(), "the failure was not reported")
		} else {
			require.Zero(t, failures(), "pass %d reported the same failure again", i)
		}
	}

	// Another failure is a new one.
	pass(backupErrorClient{MultipoolerClient: rpc, err: errors.New("certificate expired")})
	require.Equal(t, 1, failures())

	// So is the first one after a listing that worked.
	pass(rpc)
	require.Zero(t, failures())
	pass(failing)
	require.Equal(t, 1, failures())
	require.Equal(t, unhealthy.Status, verdict().Status)
}
