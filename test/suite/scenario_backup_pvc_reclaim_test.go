package suite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/testkit/ctrltest"
)

// TestSharedBackupPVCReclaim pins the reclaim of a Shard's shared backup PVC
// by the replacement its TableGroup creates after the Shard is deleted.
//
// Unless the whole MultigresCluster is being deleted, tearing a Shard down
// marks its Delete-policy PVCs multigres.com/orphan-since rather than deleting
// them (cleanupShardPVCs), leaving the deletion to multigres-gc after
// retention. The replacement Shard has the same name, so it reuses the same
// PVC, and must clear the label: otherwise multigres-gc deletes a backup volume
// in active use under a healthy shard. Server-side apply does not clear it,
// because the apply payload never mentions the label and MarkOrphan wrote it
// with a merge patch.
//
// Both subtests prove from the PVC watch that each teardown actually stamped
// the label before asserting the reclaim cleared it, so neither can pass by
// the teardown having skipped the mark.
func TestSharedBackupPVCReclaim(t *testing.T) {
	t.Run("delete and recreate, twice", testBackupPVCReclaimRepeated)
	t.Run("delete while changing the deletion policy, and back", testBackupPVCReclaimPolicyChange)
}

// testBackupPVCReclaimRepeated runs the teardown-and-reclaim cycle twice on
// one cluster. The second cycle is the return trip: a PVC the operator has
// already cleared once must be marked by the next teardown and cleared by the
// next reclaim, not left in either state by the first round.
func testBackupPVCReclaimRepeated(t *testing.T) {
	c := newCase(t)
	pvcs := c.Watch(&corev1.PersistentVolumeClaimList{})
	cluster := c.MinimalCluster("backup-reclaim")
	c.WaitForClusterHealthy(cluster)

	key := c.shardKey()
	pvcKey := c.backupPVCKey(key)
	original := &corev1.PersistentVolumeClaim{}
	c.NoError(c.Get(pvcKey, original), "get shared backup PVC %s", pvcKey.Name)

	for cycle := 1; cycle <= 2; cycle++ {
		c.recreateShard(key, cycle)
		c.WaitForClusterHealthy(cluster)
		c.RequireQuiescent(5*time.Second, 60*time.Second)

		c.requireOrphanMarkedAndCleared(pvcs, pvcKey, cycle)
		c.requireReclaimed(key, pvcKey, original.UID, true)
	}
}

// testBackupPVCReclaimPolicyChange deletes the Shard and, while it is still
// terminating, flips the cluster's PVCDeletionPolicy from Delete to Retain,
// then flips it back.
//
// The order is what makes the teardown deterministic: the TableGroup does not
// apply to a terminating Shard (observedCleanupInProgress), so the dying
// Shard keeps Delete and its teardown marks the PVC. The replacement is only
// created once the old Shard is gone, by which time the TableGroup normally
// carries Retain, so the reclaim normally runs under Retain. That second
// order is not guaranteed from outside the operator and is logged rather than
// required; TestReconcileSharedBackupPVC pins it deterministically. Under
// Retain the reclaim's apply adds no ownerRef, and reconcilePVCOwnerRefs
// skips orphan-labelled PVCs, so a label left behind would also stop the
// return trip to Delete from ever restoring the Shard's ownership.
func testBackupPVCReclaimPolicyChange(t *testing.T) {
	c := newCase(t)
	pvcs := c.Watch(&corev1.PersistentVolumeClaimList{})
	cluster := c.MinimalCluster("backup-reclaim-policy")
	c.WaitForClusterHealthy(cluster)

	key := c.shardKey()
	pvcKey := c.backupPVCKey(key)
	original := &corev1.PersistentVolumeClaim{}
	c.NoError(c.Get(pvcKey, original), "get shared backup PVC %s", pvcKey.Name)

	setPolicy := func(policy multigresv1alpha1.PVCRetentionPolicyType) {
		c.updateCluster(cluster, func(got *MultigresCluster) {
			got.Spec.PVCDeletionPolicy = &PVCDeletionPolicy{
				WhenDeleted: policy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			}
		})
	}

	replacement := c.recreateShard(key, 1, func() {
		setPolicy(multigresv1alpha1.RetainPVCRetentionPolicy)
	})
	bornWith := "unset"
	if p := replacement.Spec.PVCDeletionPolicy; p != nil {
		bornWith = string(p.WhenDeleted)
	}
	c.Logf("replacement shard uid %s was created with WhenDeleted=%s", replacement.UID, bornWith)
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(5*time.Second, 60*time.Second)
	c.requireOrphanMarkedAndCleared(pvcs, pvcKey, 1)
	c.requireReclaimed(key, pvcKey, original.UID, false)

	setPolicy(multigresv1alpha1.DeletePVCRetentionPolicy)
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(5*time.Second, 60*time.Second)
	c.requireReclaimed(key, pvcKey, original.UID, true)
}

// backupPVCKey names the shared backup PVC of the Shard at key, through the
// same builder the shard controller uses.
func (c *C) backupPVCKey(key client.ObjectKey) client.ObjectKey {
	c.Helper()
	shard := &Shard{}
	c.NoError(c.Get(key, shard), "get shard %s", key.Name)
	return client.ObjectKey{
		Namespace: key.Namespace,
		Name:      shardcontroller.BuildSharedBackupPVCName(shard),
	}
}

// recreateShard deletes the Shard at key, runs whileTerminating (if any)
// right after the delete is accepted, and returns the replacement its
// TableGroup creates under the same name.
func (c *C) recreateShard(key client.ObjectKey, cycle int, whileTerminating ...func()) *Shard {
	c.Helper()
	shard := &Shard{}
	c.NoError(c.Get(key, shard), "cycle %d: get shard %s", cycle, key.Name)
	oldUID := shard.UID
	c.NoError(c.Delete(shard), "cycle %d: delete shard %s", cycle, key.Name)
	for _, fn := range whileTerminating {
		fn()
	}
	got := &Shard{}
	c.Eventually(30*time.Second, fmt.Sprintf("cycle %d: shard %s recreated", cycle, key.Name),
		func() error {
			if err := c.Get(key, got); err != nil {
				return err
			}
			if got.UID == oldUID {
				return fmt.Errorf("shard %s not yet recreated", key.Name)
			}
			return nil
		})
	return got
}

// requireOrphanMarkedAndCleared drains the PVC watch and requires that the
// backup PVC's orphan label appeared and was then removed, in that order.
// Without the appearance the cycle never exercised the reclaim at all.
func (c *C) requireOrphanMarkedAndCleared(
	pvcs *ctrltest.Stream,
	pvcKey client.ObjectKey,
	cycle int,
) {
	c.Helper()
	events, err := pvcs.Drain()
	c.NoError(err, "cycle %d: drain PVC watch", cycle)
	marked, cleared := false, false
	for _, ev := range events {
		if ev.Key != pvcKey {
			continue
		}
		for path, tr := range ev.Transitions {
			if !strings.Contains(path, "labels") || !strings.HasSuffix(path, metadata.LabelOrphan) {
				continue
			}
			switch {
			case tr.From == "null" && tr.To != "null":
				marked = true
			case marked && tr.To == "null":
				cleared = true
			}
		}
	}
	c.True(marked, "cycle %d: teardown never marked %s %s, so nothing was reclaimed",
		cycle, pvcKey.Name, metadata.LabelOrphan)
	c.True(cleared, "cycle %d: %s was marked %s by the teardown and never cleared",
		cycle, pvcKey.Name, metadata.LabelOrphan)
}

// requireReclaimed requires that the Shard's current incarnation is using the
// original backup PVC rather than a new one, that the PVC carries no orphan
// label, and that its controller ownerRef matches the deletion policy.
func (c *C) requireReclaimed(key, pvcKey client.ObjectKey, originalUID types.UID, wantOwned bool) {
	c.Helper()
	shard := &Shard{}
	c.NoError(c.Get(key, shard), "get shard %s", key.Name)
	pvc := &corev1.PersistentVolumeClaim{}
	c.NoError(c.Get(pvcKey, pvc), "get shared backup PVC %s", pvcKey.Name)

	c.Eq(originalUID, pvc.UID, "shared backup PVC %s was replaced rather than reclaimed", pvc.Name)
	since, stale := pvc.Labels[metadata.LabelOrphan]
	c.False(stale, "shared backup PVC %s is in use by shard uid %s but carries %s=%s",
		pvc.Name, shard.UID, metadata.LabelOrphan, since)

	owner := metav1.GetControllerOf(pvc)
	if wantOwned {
		c.True(owner != nil && owner.UID == shard.UID,
			"shared backup PVC %s controller is %v, want shard uid %s", pvc.Name, owner, shard.UID)
	} else {
		c.True(owner == nil, "shared backup PVC %s under Retain has controller %v, want none",
			pvc.Name, owner)
	}
}
