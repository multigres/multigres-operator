package shard

import (
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
)

// Exercise the real data-plane phases across reconciles: quarantine consumes
// the persisted backup condition before the next backup RPC updates it.
func TestReconcileDataPlaneBackupHealthGatesQuarantine(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fullTime := now.Add(-5 * 24 * time.Hour)
	fullLabel := fullTime.Format("20060102-150405") + "F"

	for _, tc := range []struct {
		name          string
		backupType    string
		suffix        string
		age           time.Duration
		withStop      bool
		incomplete    bool
		wantRemediate bool
	}{
		{name: "fresh differential with stop timestamp", backupType: "diff", suffix: "D", age: time.Hour, withStop: true, wantRemediate: true},
		{name: "fresh differential without stop timestamp", backupType: "diff", suffix: "D", age: time.Hour, wantRemediate: true},
		{name: "fresh incremental with stop timestamp", backupType: "incr", suffix: "I", age: time.Hour, withStop: true, wantRemediate: true},
		{name: "fresh incremental without stop timestamp", backupType: "incr", suffix: "I", age: time.Hour, wantRemediate: true},
		{name: "stale differential", backupType: "diff", suffix: "D", age: 48 * time.Hour, withStop: true},
		{name: "incomplete differential", backupType: "diff", suffix: "D", age: time.Hour, withStop: true, incomplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shard := qrShard()
			badPod := qrPod(shard, 1, now.Add(-time.Hour), false)
			goodPod := qrPod(shard, 0, now.Add(-time.Hour), true)
			badPVC := qrPVC(shard, 1)
			goodPVC := qrPVC(shard, 0)
			primaryID := &clustermetadata.ID{Cell: qrCell, Name: goodPod.Name}

			backupTime := now.Add(-tc.age)
			backup := &multipoolermanagerdatapb.BackupMetadata{
				BackupId: fullLabel + "_" + backupTime.Format("20060102-150405") + tc.suffix,
				Type:     tc.backupType,
				Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			}
			if tc.withStop {
				backupTime = backupTime.Add(3 * time.Second)
				backup.StopTimestamp = timestamppb.New(backupTime)
			}
			wantType := tc.backupType
			if tc.incomplete {
				backup.Status = multipoolermanagerdatapb.BackupMetadata_INCOMPLETE
				backupTime = fullTime
				wantType = "full"
			}

			rpc := rpcclient.NewFakeClient()
			rpc.SetStatusResponse(
				topoclient.ComponentIDString(primaryID),
				&multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY,
					},
				},
			)
			rpc.GetBackupsResponses[topoclient.ComponentIDString(primaryID)] = &multipoolermanagerdatapb.GetBackupsResponse{
				Backups: []*multipoolermanagerdatapb.BackupMetadata{
					{
						BackupId:      fullLabel,
						Type:          "full",
						Status:        multipoolermanagerdatapb.BackupMetadata_COMPLETE,
						StopTimestamp: timestamppb.New(fullTime),
					},
					backup,
				},
			}
			r, c := postureTestReconciler(t, shard, rpc, badPod, goodPod, badPVC, goodPVC)
			r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
				// Each reconciliation closes its store; recreate the same topology.
				store := qrStoreWithQuarantined(t, shard, 1)
				_, err := store.UpdateMultipoolerFields(
					t.Context(),
					primaryID,
					func(pooler *clustermetadata.Multipooler) error {
						pooler.RoutingState.Role = clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY
						return nil
					},
				)
				return store, err
			}

			for pass := 1; pass <= 2; pass++ {
				if _, err := r.reconcileDataPlane(
					t.Context(),
					shard,
					renderedConfig{},
				); err != nil {
					t.Fatalf("reconcile %d: %v", pass, err)
				}
				// Reload status as the next reconciliation would. Do not manufacture
				// BackupHealthy or call Apply directly: the production path owns it.
				if err := c.Get(t.Context(), client.ObjectKeyFromObject(shard), shard); err != nil {
					t.Fatalf("get shard after reconcile %d: %v", pass, err)
				}
				condition := meta.FindStatusCondition(
					shard.Status.Conditions,
					backuphealth.ConditionHealthy,
				)
				wantStatus, wantReason := metav1.ConditionFalse, "BackupStale"
				if tc.wantRemediate {
					wantStatus, wantReason = metav1.ConditionTrue, "BackupRecent"
				}
				if condition == nil || condition.Status != wantStatus ||
					condition.Reason != wantReason {
					t.Fatalf(
						"reconcile %d: backup condition = %#v, want %s/%s",
						pass,
						condition,
						wantStatus,
						wantReason,
					)
				}
				if shard.Status.LastBackupTime == nil ||
					!shard.Status.LastBackupTime.Time.Equal(backupTime) ||
					shard.Status.LastBackupType != wantType {
					t.Fatalf(
						"reconcile %d: last backup = %v/%s, want %v/%s",
						pass,
						shard.Status.LastBackupTime,
						shard.Status.LastBackupType,
						backupTime,
						wantType,
					)
				}

				// On the first pass the gate must still block deletion, because no
				// previous backup-health observation exists. Only the second pass
				// may act, and only when the completed backup is recent.
				wantExists := pass == 1 || !tc.wantRemediate
				if got := exists(
					t,
					r,
					&corev1.Pod{},
					client.ObjectKeyFromObject(badPod),
				); got != wantExists {
					t.Errorf(
						"reconcile %d: quarantined pod exists = %v, want %v",
						pass,
						got,
						wantExists,
					)
				}
				if got := exists(
					t,
					r,
					&corev1.PersistentVolumeClaim{},
					client.ObjectKeyFromObject(badPVC),
				); got != wantExists {
					t.Errorf(
						"reconcile %d: quarantined data PVC exists = %v, want %v",
						pass,
						got,
						wantExists,
					)
				}
				if !exists(t, r, &corev1.Pod{}, client.ObjectKeyFromObject(goodPod)) ||
					!exists(
						t,
						r,
						&corev1.PersistentVolumeClaim{},
						client.ObjectKeyFromObject(goodPVC),
					) {
					t.Errorf(
						"reconcile %d: healthy primary pod and PVC must remain untouched",
						pass,
					)
				}
			}
		})
	}
}
