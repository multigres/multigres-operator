package multigrescluster

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

const backupScheduleCRDDir = "../../../../config/crd/bases"

// Adding, changing and removing spec.backupSchedules entries is reflected in
// the MultigresBackupSchedule objects; user-created schedules are left alone.
func TestReconcileBackupSchedules(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mgc", Namespace: "proj", UID: "cluster-uid"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			BackupSchedules: []multigresv1alpha1.BackupScheduleEntry{
				{
					Name: "daily-differential",
					BackupScheduleTemplate: multigresv1alpha1.BackupScheduleTemplate{
						Schedule: "0 4 * * 0-5", Type: multigresv1alpha1.BackupKindDifferential,
					},
				},
				{
					Name: "weekly-full",
					BackupScheduleTemplate: multigresv1alpha1.BackupScheduleTemplate{
						Schedule: "0 4 * * 6", Type: multigresv1alpha1.BackupKindFull,
					},
				},
			},
		},
	}
	userOwned := &multigresv1alpha1.MultigresBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mgc-mine", Namespace: "proj",
			Labels: map[string]string{"multigres.com/cluster": "mgc"},
		},
		Spec: multigresv1alpha1.MultigresBackupScheduleSpec{
			ClusterRef: multigresv1alpha1.BackupClusterReference{Name: "mgc"},
			BackupScheduleTemplate: multigresv1alpha1.BackupScheduleTemplate{
				Schedule: "0 0 * * *", Type: multigresv1alpha1.BackupKindIncremental,
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, userOwned).
		WithTypeConverters(testutil.TypeConverters(t, backupScheduleCRDDir)...).
		Build()
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	get := func(name string) *multigresv1alpha1.MultigresBackupSchedule {
		t.Helper()
		s := &multigresv1alpha1.MultigresBackupSchedule{}
		err := c.Get(ctx, client.ObjectKey{Namespace: "proj", Name: name}, s)
		if client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		if err != nil {
			return nil
		}
		return s
	}

	if err := r.reconcileBackupSchedules(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	weekly := get("mgc-weekly-full")
	if weekly == nil || weekly.Spec.Type != multigresv1alpha1.BackupKindFull ||
		weekly.Spec.ClusterRef.Name != "mgc" || weekly.Spec.Schedule != "0 4 * * 6" {
		t.Fatalf("weekly = %+v", weekly)
	}
	if owner := metav1.GetControllerOf(weekly); owner == nil || owner.UID != "cluster-uid" {
		t.Fatalf("owner = %v", weekly.OwnerReferences)
	}
	if get("mgc-daily-differential") == nil {
		t.Fatal("daily schedule not created")
	}

	// Change one entry and remove the other.
	cluster.Spec.BackupSchedules = cluster.Spec.BackupSchedules[1:]
	cluster.Spec.BackupSchedules[0].Schedule = "30 5 * * 6"
	if err := r.reconcileBackupSchedules(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if s := get("mgc-weekly-full"); s == nil || s.Spec.Schedule != "30 5 * * 6" {
		t.Fatalf("weekly not updated: %+v", s)
	}
	if get("mgc-daily-differential") != nil {
		t.Fatal("removed entry's schedule still exists")
	}
	if get("mgc-mine") == nil {
		t.Fatal("user-created schedule was deleted")
	}
}
