package suite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/testkit/ctrltest"
)

// TestBackupStorageShrink drives backup storage edits the API server will not
// apply to an existing PVC at a converged cluster: a smaller size alone
// (TestTransitions/Backup), together with another spec change in the same
// update, as the first leg of a shrink-then-grow, and on a Lite shard, which
// reconciles the same shared backup PVC on its own path; a larger size on a
// PVC whose class cannot expand; and a class that does not exist.
//
// In every shape the operator has to keep the PVC as it is, report that on
// the Shard, and carry on with everything else the update asked for. Quiescence is
// the assertion that it carries on: the write recorder counts writes the API
// server refuses, so a shard retrying a rejected apply never goes quiet.
func TestBackupStorageShrink(t *testing.T) {
	t.Run("WithOtherChange", testBackupShrinkWithOtherChange)
	t.Run("ThenGrow", testBackupShrinkThenGrow)
	t.Run("Lite", testBackupShrinkLite)
	t.Run("GrowWithoutExpansion", testBackupGrowWithoutExpansion)
	t.Run("GrowOnceClassAllowsExpansion", testBackupGrowOnceClassAllowsExpansion)
	t.Run("MissingClassWithOtherChange", testBackupMissingClassWithOtherChange)
}

func filesystemBackup(size, class string) *multigresv1alpha1.BackupConfig {
	return &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
			Storage: multigresv1alpha1.StorageSpec{Size: size, Class: class},
		},
	}
}

// backupPVCRequests reads the storage request of every shared backup PVC in
// the namespace, keyed by PVC name.
func (c *C) backupPVCRequests() map[string]string {
	c.Helper()
	out := map[string]string{}
	for name, request := range c.pvcRequests() {
		if strings.HasPrefix(name, "backup-data-") {
			out[name] = request
		}
	}
	if len(out) == 0 {
		c.Fatalf("no shared backup PVCs in %s", c.NS)
	}
	return out
}

// waitForStorageSpecIgnored waits until every Shard reports
// StorageSpecIgnored with the given status for its current generation, and,
// when mentions is non-empty, with a message naming it.
func (c *C) waitForStorageSpecIgnored(want bool, mentions string) {
	c.Helper()
	status := metav1.ConditionFalse
	if want {
		status = metav1.ConditionTrue
	}
	c.Eventually(30*time.Second, fmt.Sprintf("every Shard to report StorageSpecIgnored=%s", status),
		func() error {
			shards := &ShardList{}
			if err := c.List(shards); err != nil {
				return err
			}
			if len(shards.Items) == 0 {
				return fmt.Errorf("no Shards yet")
			}
			for _, shard := range shards.Items {
				cond := meta.FindStatusCondition(
					shard.Status.Conditions,
					multigresv1alpha1.ConditionStorageSpecIgnored,
				)
				switch {
				case cond == nil:
					return fmt.Errorf("Shard %s has no StorageSpecIgnored condition", shard.Name)
				case cond.Status != status || cond.ObservedGeneration != shard.Generation:
					return fmt.Errorf(
						"Shard %s StorageSpecIgnored=%s at generation %d, want %s at %d: %s",
						shard.Name,
						cond.Status,
						cond.ObservedGeneration,
						status,
						shard.Generation,
						cond.Message,
					)
				case !strings.Contains(cond.Message, mentions):
					return fmt.Errorf("Shard %s StorageSpecIgnored message %q does not mention %q",
						shard.Name, cond.Message, mentions)
				}
			}
			return nil
		})
}

// shardOwnedPVCs names the PVCs in the namespace that carry a Shard controller
// ownerRef. The global topology's etcd PVCs belong to its StatefulSet and are
// never Shard-owned, so they never appear here.
func (c *C) shardOwnedPVCs() []string {
	c.Helper()
	pvcs := &corev1.PersistentVolumeClaimList{}
	c.NoError(c.List(pvcs), "list PVCs")
	var owned []string
	for _, pvc := range pvcs.Items {
		if owner := metav1.GetControllerOf(&pvc); owner != nil && owner.Kind == "Shard" {
			owned = append(owned, pvc.Name)
		}
	}
	return owned
}

// testBackupShrinkWithOtherChange lowers the backup size and switches the PVC
// deletion policy to Retain in one update. The policy lands through
// reconcilePVCOwnerRefs, which Reconcile runs after the shared backup PVC, so
// the ownerRefs coming off is direct evidence that the shrink does not stop
// the steps behind it.
func testBackupShrinkWithOtherChange(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("backupmulti")
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	backupBefore := c.backupPVCRequests()
	c.NotEmpty(c.shardOwnedPVCs(), "Delete policy left no Shard-owned PVCs to release")

	c.updateCluster(cluster, func(c *MultigresCluster) {
		c.Spec.Backup = filesystemBackup("5Gi", "")
		c.Spec.PVCDeletionPolicy = &PVCDeletionPolicy{
			WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
			WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
		}
	})

	c.Eventually(30*time.Second, "Retain to release every Shard-owned PVC", func() error {
		if owned := c.shardOwnedPVCs(); len(owned) > 0 {
			return fmt.Errorf("still Shard-owned: %v", owned)
		}
		return nil
	})
	c.waitForStorageSpecIgnored(true, "backup-data-")
	c.RequireQuiescent(5*time.Second, 30*time.Second)
	c.Check().EqDiff(backupBefore, c.backupPVCRequests(), "backup PVC request moved")
}

// testBackupShrinkThenGrow shrinks the backup size, grows it past the
// original, then sets it back to the original, which is a shrink against the
// grown PVC. The backup PVC's class allows expansion so the growth is one the
// API server accepts.
//
// testkit's DataPlaneSim binds a PVC once and never models a resize, so
// status.capacity stays at the size the PVC was created with. That does not
// matter to the shared backup PVC, which nothing waits on, and the request,
// which is what the operator writes, is asserted directly.
func testBackupShrinkThenGrow(t *testing.T) {
	c := newCase(t)
	class := c.expandableStorageClass("backup-expandable")
	cluster := c.newCluster("backupgrow", func(spec *MultigresClusterSpec) {
		spec.Backup = filesystemBackup("10Gi", class)
	})
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	setSize := func(size string) {
		c.updateCluster(cluster, func(c *MultigresCluster) {
			c.Spec.Backup = filesystemBackup(size, class)
		})
	}
	waitForRequest := func(want string) {
		c.Helper()
		c.Eventually(30*time.Second, "backup PVC request "+want, func() error {
			for name, request := range c.backupPVCRequests() {
				if request != want {
					return fmt.Errorf("PVC %s requests %s", name, request)
				}
			}
			return nil
		})
	}

	setSize("5Gi")
	c.waitForStorageSpecIgnored(true, "spec asks for 5Gi")
	c.RequireQuiescent(5*time.Second, 30*time.Second)
	waitForRequest("10Gi")

	setSize("20Gi")
	waitForRequest("20Gi")
	c.waitForStorageSpecIgnored(false, "")
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	setSize("10Gi")
	c.waitForStorageSpecIgnored(true, "keeps storage request 20Gi")
	c.RequireQuiescent(5*time.Second, 30*time.Second)
	waitForRequest("20Gi")
}

// testBackupShrinkLite lowers the backup size and switches the PVC deletion
// policy to Retain on a Lite cluster in one update. reconcileLite applies the
// shared backup PVC before the PVC ownerRefs, the Lite pod and its config, so
// this is the same shape on the other branch of Reconcile.
//
// A Lite cluster in this suite stays Progressing: the pooler fake does not
// report the Lite pooler as PRIMARY, so the shard requeues every
// litePrimaryRequeue and neither WaitForClusterHealthy nor quiescence is
// reachable. In their place:
// the ownerRefs coming off shows a pass got past the backup PVC, and the
// write log, which keeps refused writes, shows no PVC write was refused on
// the way there.
func testBackupShrinkLite(t *testing.T) {
	c := newCase(t)
	cluster := c.newCluster("backuplite", func(spec *MultigresClusterSpec) {
		spec.Mode = multigresv1alpha1.ModeLite
	})
	c.Eventually(30*time.Second, "the Lite shard's PVCs to be bound and Shard-owned", func() error {
		pvcs := &corev1.PersistentVolumeClaimList{}
		if err := c.List(pvcs); err != nil {
			return err
		}
		owned := c.shardOwnedPVCs()
		if len(owned) < 2 {
			return fmt.Errorf("Shard-owned PVCs %v, want the backup and the data PVC", owned)
		}
		for _, pvc := range pvcs.Items {
			if pvc.Status.Phase != corev1.ClaimBound {
				return fmt.Errorf("PVC %s is %s", pvc.Name, pvc.Status.Phase)
			}
		}
		return nil
	})
	backupBefore := c.backupPVCRequests()
	from := len(Suite.Ops.All())

	c.updateCluster(cluster, func(c *MultigresCluster) {
		c.Spec.Backup = filesystemBackup("5Gi", "")
		c.Spec.PVCDeletionPolicy = &PVCDeletionPolicy{
			WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
			WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
		}
	})
	c.waitForStorageSpecIgnored(true, "backup-data-")
	c.Eventually(30*time.Second, "Retain to release every Shard-owned PVC", func() error {
		if owned := c.shardOwnedPVCs(); len(owned) > 0 {
			return fmt.Errorf("still Shard-owned: %v", owned)
		}
		return nil
	})

	for _, op := range Suite.Ops.All()[from:] {
		if op.Key.Namespace == c.NS && op.Err != nil &&
			ctrltest.KindSuffix(op.Kind) == "PersistentVolumeClaim" {
			c.Errorf("refused PVC write after the shrink: %v", op)
		}
	}
	c.Check().EqDiff(backupBefore, c.backupPVCRequests(), "backup PVC request moved")
}

// testBackupGrowWithoutExpansion raises the backup size on MinimalCluster,
// whose PVCs have no StorageClass. The PersistentVolumeClaimResize admission
// plugin refuses to grow such a PVC, so this is the larger-size counterpart
// of the shrink: kept, reported, and nothing behind it held up.
func testBackupGrowWithoutExpansion(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("backupnogrow")
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(5*time.Second, 30*time.Second)
	backupBefore := c.backupPVCRequests()

	c.updateCluster(cluster, func(c *MultigresCluster) {
		c.Spec.Backup = filesystemBackup("20Gi", "")
	})
	c.waitForStorageSpecIgnored(true, "the PVC has no StorageClass, so it cannot be expanded")
	c.RequireQuiescent(5*time.Second, 30*time.Second)
	c.Check().EqDiff(backupBefore, c.backupPVCRequests(), "backup PVC request moved")

	c.updateCluster(cluster, func(c *MultigresCluster) {
		c.Spec.Backup = nil
	})
	c.waitForStorageSpecIgnored(false, "")
	c.RequireQuiescent(5*time.Second, 30*time.Second)
}

// testBackupMissingClassWithOtherChange points the backup storage at a
// StorageClass that does not exist and switches the PVC deletion policy to
// Retain in one update. The missing class only matters to a backup PVC that
// has yet to be created; this one exists and keeps its class, so the Retain
// has to land.
func testBackupMissingClassWithOtherChange(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("backupnoclass")
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(5*time.Second, 30*time.Second)
	c.NotEmpty(c.shardOwnedPVCs(), "Delete policy left no Shard-owned PVCs to release")

	c.updateCluster(cluster, func(c *MultigresCluster) {
		c.Spec.Backup = filesystemBackup("10Gi", c.Namespace+"-no-such-class")
		c.Spec.PVCDeletionPolicy = &PVCDeletionPolicy{
			WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
			WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
		}
	})
	c.Eventually(30*time.Second, "Retain to release every Shard-owned PVC", func() error {
		if owned := c.shardOwnedPVCs(); len(owned) > 0 {
			return fmt.Errorf("still Shard-owned: %v", owned)
		}
		return nil
	})
	c.waitForStorageSpecIgnored(true, "-no-such-class")
	c.RequireQuiescent(5*time.Second, 30*time.Second)
}

// testBackupGrowOnceClassAllowsExpansion grows the backup size on a class that
// does not allow expansion, then has an admin turn expansion on, with no
// further edit to the cluster. A kept growth produces no error and so no
// retry; the StorageClass event is what has to bring the shard back to apply
// it.
func testBackupGrowOnceClassAllowsExpansion(t *testing.T) {
	c := newCase(t)
	sc := c.storageClass("backup-fixed", false)
	cluster := c.newCluster("backuplategrow", func(spec *MultigresClusterSpec) {
		spec.Backup = filesystemBackup("10Gi", sc.Name)
	})
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	c.updateCluster(cluster, func(c *MultigresCluster) {
		c.Spec.Backup = filesystemBackup("20Gi", sc.Name)
	})
	c.waitForStorageSpecIgnored(
		true,
		fmt.Sprintf("StorageClass %q does not allow volume expansion", sc.Name),
	)
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	c.NoError(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		got := &storagev1.StorageClass{}
		if err := c.Get(client.ObjectKeyFromObject(sc), got); err != nil {
			return err
		}
		got.AllowVolumeExpansion = ptr.To(true)
		return c.Update(got)
	}), "allow expansion on %s", sc.Name)

	c.Eventually(30*time.Second, "backup PVC request 20Gi", func() error {
		for name, request := range c.backupPVCRequests() {
			if request != "20Gi" {
				return fmt.Errorf("PVC %s requests %s", name, request)
			}
		}
		return nil
	})
	c.waitForStorageSpecIgnored(false, "")
	c.RequireQuiescent(5*time.Second, 30*time.Second)
}
