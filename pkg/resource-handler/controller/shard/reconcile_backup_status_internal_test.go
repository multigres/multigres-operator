package shard

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

type beforeBackupClient struct {
	rpcclient.MultipoolerClient
	before func()
}

func (c beforeBackupClient) GetBackups(
	ctx context.Context,
	pooler *clustermetadata.Multipooler,
	req *multipoolermanagerdatapb.GetBackupsRequest,
) (*multipoolermanagerdatapb.GetBackupsResponse, error) {
	c.before()
	return c.MultipoolerClient.GetBackups(ctx, pooler, req)
}

// A guard write after the reconciler's snapshot must survive backup-health
// publication, both when the backup RPC succeeds and when it fails.
func TestReconcileDataPlaneBackupHealthPreservesStorageGuard(t *testing.T) {
	for _, rpcFails := range []bool{false, true} {
		name := "successful backup RPC"
		if rpcFails {
			name = "failed backup RPC"
		}
		t.Run(name, func(t *testing.T) {
			shard := postureTestShard()
			shard.Status.Conditions = []metav1.Condition{{
				Type: conditionStorageClassValid, Status: metav1.ConditionTrue,
				Reason: storageClassFoundReason, Message: "StorageClass found",
				LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
			}}
			store, poolerID := postureTestStore(t)
			_, err := store.UpdateMultipoolerFields(t.Context(),
				&clustermetadata.ID{Cell: "cell1", Name: "pooler-0"},
				func(pooler *clustermetadata.Multipooler) error {
					pooler.RoutingState.Role = clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			baseRPC := rpcclient.NewFakeClient()
			baseRPC.SetStatusResponse(poolerID, &multipoolermanagerdatapb.StatusResponse{
				Status: &multipoolermanagerdatapb.Status{
					PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY,
				},
			})
			baseRPC.GetBackupsResponses[poolerID] = &multipoolermanagerdatapb.GetBackupsResponse{
				Backups: []*multipoolermanagerdatapb.BackupMetadata{{
					Type: "full", Status: multipoolermanagerdatapb.BackupMetadata_COMPLETE,
					StopTimestamp: timestamppb.New(time.Now().Add(-time.Hour)),
				}},
			}
			var rpc rpcclient.MultipoolerClient = baseRPC
			if rpcFails {
				rpc = backupErrorClient{
					MultipoolerClient: baseRPC,
					err:               errors.New("backup RPC unavailable"),
				}
			}
			var r *ShardReconciler
			var wantGuard *metav1.Condition
			rpc = beforeBackupClient{MultipoolerClient: rpc, before: func() {
				// Publish a newer guard verdict while the data-plane pass still
				// holds the original condition. No sleeps or cache timing needed.
				if err := r.setStorageClassCondition(t.Context(), shard, storageClassCheck{
					status: metav1.ConditionFalse, reason: storageClassNotFoundReason,
					message: "StorageClass no longer exists",
				}); err != nil {
					t.Fatal(err)
				}
				latest := &multigresv1alpha1.Shard{}
				if err := r.Get(
					t.Context(),
					client.ObjectKeyFromObject(shard),
					latest,
				); err != nil {
					t.Fatal(err)
				}
				wantGuard = meta.FindStatusCondition(latest.Status.Conditions, conditionStorageClassValid).
					DeepCopy()
			}}
			// Use the CRD's keyed condition-list schema so SSA ownership and
			// omission behave as they do on the API server.
			scheme := postureTestScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithTypeConverters(testutil.TypeConverters(t, "../../../../config/crd/bases")...).
				WithObjects(shard, postureTestPod()).WithStatusSubresource(shard).
				WithReturnManagedFields().Build()
			r = &ShardReconciler{
				Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20),
				PoolerClients: poolerclient.Static(rpc),
			}
			r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) { return store, nil }
			if _, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{}); err != nil {
				t.Fatal(err)
			}
			got := &multigresv1alpha1.Shard{}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(shard), got); err != nil {
				t.Fatal(err)
			}
			if wantGuard == nil {
				t.Fatal("backup RPC did not run")
			}
			guardPath := ownership.ConditionPath(conditionStorageClassValid)
			if !ownership.Owns(got, ownership.ManagerShardGuard, guardPath) ||
				ownership.Owns(got, ownership.ManagerShard, guardPath) {
				t.Errorf(
					"storage guard condition must remain exclusively owned by %s",
					ownership.ManagerShardGuard,
				)
			}
			if guard := meta.FindStatusCondition(
				got.Status.Conditions,
				conditionStorageClassValid,
			); !reflect.DeepEqual(
				guard,
				wantGuard,
			) {
				t.Errorf(
					"storage guard overwritten by backup health: got %#v, want %#v",
					guard,
					wantGuard,
				)
			}
			wantStatus := metav1.ConditionTrue
			if rpcFails {
				wantStatus = metav1.ConditionUnknown
			}
			if backup := meta.FindStatusCondition(
				got.Status.Conditions,
				backuphealth.ConditionHealthy,
			); backup == nil ||
				backup.Status != wantStatus {
				t.Errorf("backup condition = %#v, want %s", backup, wantStatus)
			}
		})
	}
}
