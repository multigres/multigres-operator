package suite

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/gc"
	gcpvc "github.com/multigres/multigres-operator/pkg/gc/pvc"
	"github.com/multigres/multigres-operator/pkg/resolver"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

// TestTerminatingPVC covers a shard whose PVCs are deleted while its pods use
// them, and the multigres-gc deletion that can cause it. A deleted PVC lingers
// under pvc-protection, data intact, until the last pod using it stops; after
// that a PVC of the same name is a new, empty one. The operator must neither
// report such a shard Healthy nor move its pods off such a PVC, and once the
// PVC is gone it must recover with a fresh one.
func TestTerminatingPVC(t *testing.T) {
	t.Run("backup PVC deleted under a running shard", testBackupPVCDeletedUnderRunningShard)
	t.Run("multigres-gc deletes a PVC as a new Shard reclaims it", testGCDeletionRacesReclaim)
	t.Run("stale orphan label on an in-use PVC", testStaleOrphanLabelInUse)
	t.Run("Lite data PVC deleted under its pod", testLiteDataPVCDeleted)
}

// testBackupPVCDeletedUnderRunningShard deletes a converged shard's shared
// backup PVC, as multigres-gc did on any shard that had reclaimed it from an
// earlier incarnation, then plays the person who decides: deletes the pods.
func testBackupPVCDeletedUnderRunningShard(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("backup-terminating")
	c.WaitForClusterHealthy(cluster)
	key := c.shardKey()
	shard := &Shard{}
	c.NoError(c.Get(key, shard), "get shard")
	backupName := shardcontroller.BuildSharedBackupPVCName(shard)

	original := c.requirePVCProtected(backupName)
	podsBefore := c.shardPodUIDs(key)
	c.True(len(podsBefore) > 0, "the converged shard has pool pods")

	deletedAt := time.Now()
	c.NoError(c.Delete(original), "delete the shared backup PVC")

	t.Run("reported, not Healthy", func(t *testing.T) {
		c := c.Sub(t)
		c.requirePVCTerminatingReported(key, backupName)
	})

	t.Run("left alone", func(t *testing.T) {
		c := c.Sub(t)
		c.awaitShardSettled(key, deletedAt, settlePasses)
		for _, op := range Suite.Ops.OpsInNamespace(c.NS) {
			if op.Controller == "shard" && op.At.After(deletedAt) &&
				op.Kind == "PersistentVolumeClaim" && op.Key.Name == backupName {
				c.Errorf("shard wrote %s %s after it was deleted", op.Verb, backupName)
			}
		}
		c.EqDiff(podsBefore, c.shardPodUIDs(key),
			"no pool pod is deleted or replaced while the backup PVC is held by them")
		c.requirePVCTerminatingReported(key, backupName)
	})

	t.Run("recovers with a new, empty PVC once the pods are deleted", func(t *testing.T) {
		c := c.Sub(t)
		// pvcProtectionSim finishes the deletion once no pod uses the PVC.
		c.deleteShardPods(key)
		c.Eventually(30*time.Second, "a new shared backup PVC", func() error {
			got := &corev1.PersistentVolumeClaim{}
			if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: backupName}, got); err != nil {
				return err
			}
			if got.UID == original.UID {
				return fmt.Errorf("still the deleted PVC")
			}
			return nil
		})
		c.WaitForClusterHealthy(cluster)
		c.requirePVCTerminatingCleared(key)
	})
}

// testGCDeletionRacesReclaim commits multigres-gc's deletion of each orphaned
// PVC between the reconcile that read it and that reconcile's patch clearing
// its orphan label, as a new incarnation of a Shard reclaims its storage. The
// deletion is a legitimate retention expiry, so the right outcome is the
// deletion standing and the shard starting on new, empty PVCs; the wrong one,
// which an unconditional patch produces, is the shard putting the deleted
// PVCs back into service and reporting Healthy on storage that is about to
// vanish.
func testGCDeletionRacesReclaim(t *testing.T) {
	c := newCase(t)
	c.HoldPVCProtection(t)
	cluster := c.MinimalCluster("gc-reclaim-race")
	c.WaitForClusterHealthy(cluster)
	key := c.shardKey()
	shard := &Shard{}
	c.NoError(c.Get(key, shard), "get shard")
	pvcs := &corev1.PersistentVolumeClaimList{}
	c.NoError(c.List(pvcs, client.MatchingLabels{
		metadata.LabelMultigresShard:   string(shard.Spec.ShardName),
		metadata.LabelMultigresCluster: shard.Labels[metadata.LabelMultigresCluster],
	}), "list shard PVCs")
	c.True(len(pvcs.Items) > 1, "the shard has a backup PVC and pool PVCs")
	shardPVCs := make([]string, 0, len(pvcs.Items))
	for i := range pvcs.Items {
		shardPVCs = append(shardPVCs, pvcs.Items[i].Name)
	}

	var (
		mu       sync.Mutex
		deleted  = map[string]types.UID{}
		hookErrs []error
	)
	c.BeforeShardPatch(t, func(ctx context.Context, obj client.Object, patch client.Patch) {
		pvc, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok || !clearsOrphanLabel(patch, obj) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if _, done := deleted[pvc.Name]; done {
			return
		}
		// What multigres-gc does: list, then delete conditioned on the
		// listed resourceVersion. Another writer (the data plane sim
		// updating status) can land between the two, so retry as a fresh
		// GC run would.
		err := retry.OnError(retry.DefaultRetry, apierrors.IsConflict, func() error {
			live := &corev1.PersistentVolumeClaim{}
			if err := Suite.Client.Get(ctx, client.ObjectKeyFromObject(pvc), live); err != nil {
				return err
			}
			if err := Suite.Client.Delete(ctx, live, client.Preconditions{
				ResourceVersion: &live.ResourceVersion,
			}); err != nil {
				return err
			}
			deleted[pvc.Name] = live.UID
			return nil
		})
		if err != nil {
			hookErrs = append(hookErrs, fmt.Errorf("delete %s: %w", pvc.Name, err))
		}
	})

	// Tear the Shard down and let its TableGroup recreate it: the teardown
	// orphans its PVCs, the new incarnation reclaims them.
	oldUID := shard.UID
	c.NoError(c.Delete(shard), "delete shard")
	c.Eventually(30*time.Second, "the TableGroup to recreate the Shard", func() error {
		got := &Shard{}
		if err := c.Get(key, got); err != nil {
			return err
		}
		if got.UID == oldUID {
			return fmt.Errorf("not recreated yet")
		}
		return nil
	})

	c.Eventually(30*time.Second, "every reclaim to meet a deletion", func() error {
		mu.Lock()
		defer mu.Unlock()
		if len(hookErrs) > 0 {
			return fmt.Errorf("hook: %v", hookErrs)
		}
		for _, name := range shardPVCs {
			if _, ok := deleted[name]; !ok {
				return fmt.Errorf("%s not raced yet; raced %v", name, deleted)
			}
		}
		return nil
	})
	patchHooks.Delete(c.NS)
	racedAt := time.Now()

	t.Run("no pod starts on a PVC being deleted", func(t *testing.T) {
		c := c.Sub(t)
		c.awaitShardSettled(key, racedAt, settlePasses)
		mu.Lock()
		raced := maps2slice(deleted)
		mu.Unlock()
		for _, name := range raced {
			pvc := &corev1.PersistentVolumeClaim{}
			c.NoError(c.Get(client.ObjectKey{Namespace: c.NS, Name: name}, pvc), "get %s", name)
			c.True(pvcutil.IsTerminating(pvc), "%s stays deleted", name)
		}
		pods := &corev1.PodList{}
		c.NoError(c.List(pods), "list pods")
		for i := range pods.Items {
			for _, vol := range pods.Items[i].Spec.Volumes {
				if vol.PersistentVolumeClaim != nil &&
					slices.Contains(raced, vol.PersistentVolumeClaim.ClaimName) {
					c.Errorf("pod %s mounts %s, which is being deleted",
						pods.Items[i].Name, vol.PersistentVolumeClaim.ClaimName)
				}
			}
		}
		got := &Shard{}
		c.NoError(c.Get(key, got), "get shard")
		c.True(got.Status.Phase != multigresv1alpha1.PhaseHealthy,
			"the shard is not Healthy without its storage; phase %q", got.Status.Phase)
	})

	t.Run("recovers on new PVCs once the deletions finish", func(t *testing.T) {
		c := c.Sub(t)
		mu.Lock()
		raced := map[string]types.UID{}
		for k, v := range deleted {
			raced[k] = v
		}
		mu.Unlock()
		for name, uid := range raced {
			c.releasePVCProtection(name, uid)
		}
		c.WaitForClusterHealthy(cluster)
		for name, uid := range raced {
			got := &corev1.PersistentVolumeClaim{}
			c.NoError(c.Get(client.ObjectKey{Namespace: c.NS, Name: name}, got), "get %s", name)
			c.True(got.UID != uid, "%s is a new PVC", name)
			c.False(pvcutil.HasOrphanLabel(got), "%s is not marked orphan", name)
		}
		c.requirePVCTerminatingCleared(key)
	})
}

// testStaleOrphanLabelInUse covers a shard whose in-use backup PVC carries an
// orphan label it should not, which a reclaim that left the label in place
// produced on every Shard recreated over its predecessor's PVCs. multigres-gc
// runs at the worst moment, after the reconcile has read the PVC and before it
// clears the label, and must leave the PVC alone because pods use it; the
// shard then clears the label.
func testStaleOrphanLabelInUse(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("stale-orphan")
	c.WaitForClusterHealthy(cluster)
	key := c.shardKey()
	shard := &Shard{}
	c.NoError(c.Get(key, shard), "get shard")
	backupName := shardcontroller.BuildSharedBackupPVCName(shard)
	pvcKey := client.ObjectKey{Namespace: c.NS, Name: backupName}

	var (
		mu      sync.Mutex
		results []*gc.Results
		gcErrs  []error
	)
	c.BeforeShardPatch(t, func(ctx context.Context, obj client.Object, patch client.Patch) {
		if obj.GetName() != backupName || !clearsOrphanLabel(patch, obj) {
			return
		}
		res, err := gcpvc.New(Suite.Client, testr.New(t), gc.Options{
			Namespace: obj.GetNamespace(),
			Retention: time.Hour,
		}).Clean(ctx)
		mu.Lock()
		defer mu.Unlock()
		results = append(results, res)
		if err != nil {
			gcErrs = append(gcErrs, err)
		}
	})

	pvc := &corev1.PersistentVolumeClaim{}
	c.NoError(c.Get(pvcKey, pvc), "get backup PVC")
	base := pvc.DeepCopy()
	pvc.Labels[metadata.LabelOrphan] = "2020-01-01T00-00-00Z"
	c.NoError(c.Patch(pvc, client.MergeFrom(base)), "label the in-use backup PVC orphan")

	c.Eventually(15*time.Second, "the shard to clear the orphan label", func() error {
		got := &corev1.PersistentVolumeClaim{}
		if err := c.Get(pvcKey, got); err != nil {
			return err
		}
		if since, ok := got.Labels[metadata.LabelOrphan]; ok {
			return fmt.Errorf("still labelled %s", since)
		}
		return nil
	})

	mu.Lock()
	defer mu.Unlock()
	c.Empty(gcErrs, "multigres-gc runs cleanly")
	c.NotEmpty(results, "multigres-gc ran between the read and the clearing patch")
	for _, res := range results {
		c.Eq(0, res.Deleted, "multigres-gc deletes nothing a pod uses")
		c.Eq(1, res.InUse, "multigres-gc reports the in-use orphan")
	}
	c.NoError(c.Get(pvcKey, pvc), "get backup PVC")
	c.False(pvcutil.IsTerminating(pvc), "the backup PVC is not deleted")
	c.WaitForClusterHealthy(cluster)
}

// settlePasses is how many complete shard reconciles a test lets run before
// asserting that something did not happen.
const settlePasses = 20

// awaitShardSettled waits until the shard has had its chance to act: n
// reconciles of key that started after since and returned without error, or
// a namespace with nothing left to do. A shard holding off on a PVC being
// deleted may do either: it requeues while it waits on other things (pods
// not ready, a drain cut short) and goes quiet when only the PVC's deletion
// event is left to wake it.
func (c *C) awaitShardSettled(key client.ObjectKey, since time.Time, n int) {
	c.Helper()
	c.Eventually(
		60*time.Second,
		fmt.Sprintf("%d clean shard passes or quiescence", n),
		func() error {
			got := 0
			for _, rec := range Suite.Reconciles.InNamespace(c.NS) {
				if rec.Controller == "shard" && rec.Request.NamespacedName == key &&
					rec.Start.After(since) && rec.Err == nil && !rec.Gated {
					got++
				}
			}
			if got >= n {
				return nil
			}
			if err := c.TryQuiescent(2*time.Second, 3*time.Second); err != nil {
				return fmt.Errorf("%d passes so far, and not quiet: %w", got, err)
			}
			return nil
		},
	)
}

// testLiteDataPVCDeleted deletes a Lite shard's data PVC under its only pod,
// then changes the pool so that pod drifts. A Lite shard replaces a drifted pod
// directly, with no drain and no cohort check, so this is the one place in the
// suite a voluntary replacement actually happens; it must not happen here.
// Pool shards gate the same replacement in canStartDisruption, which the
// suite cannot reach because its pooler fake refuses every drain.
func testLiteDataPVCDeleted(t *testing.T) {
	c := newCase(t)
	clusterName := "lite-terminating"
	c.newCluster(clusterName, func(s *MultigresClusterSpec) {
		s.Mode = multigresv1alpha1.ModeLite
		s.Databases = []DatabaseConfig{{
			Name:    resolver.DefaultSystemDatabaseName,
			Default: true,
			TableGroups: []TableGroupConfig{{
				Name:    resolver.DefaultSystemTableGroupName,
				Default: true,
				Shards: []ShardConfig{{
					Name: lifecycleShardName,
					Spec: &ShardInlineSpec{
						Pools: map[PoolName]PoolSpec{
							resolver.DefaultPoolName: {
								Type:            "readWrite",
								Cells:           []CellName{defaultSimCell},
								ReplicasPerCell: ptr.To(int32(1)),
							},
						},
					},
				}},
			}},
		}}
	})
	key := c.shardKey()
	shard := &Shard{}
	c.NoError(c.Get(key, shard), "get shard")
	dataName := shardcontroller.BuildPoolDataPVCName(
		shard, string(resolver.DefaultPoolName), defaultSimCell, 0,
	)
	podKey := client.ObjectKey{
		Namespace: c.NS,
		Name: shardcontroller.BuildPoolPodName(
			shard, string(resolver.DefaultPoolName), defaultSimCell, 0,
		),
	}
	pod := &corev1.Pod{}
	c.Eventually(30*time.Second, "the Lite pod to run on its data PVC", func() error {
		if err := c.Get(podKey, pod); err != nil {
			return err
		}
		if !podReady(pod) {
			return fmt.Errorf("pod not ready")
		}
		return nil
	})
	original := c.requirePVCProtected(dataName)

	c.NoError(c.Delete(original), "delete the Lite data PVC")
	c.requirePVCTerminatingReported(key, dataName)

	t.Run("a drifted pod is not replaced", func(t *testing.T) {
		c := c.Sub(t)
		changedAt := time.Now()
		c.updateLifecyclePool(clusterName, func(p *PoolSpec) {
			p.Postgres.Resources = lifecycleResources("100m", "128Mi", "200m", "256Mi")
		})
		c.Eventually(30*time.Second, "the shard to see the pod drifted", func() error {
			got := &Shard{}
			if err := c.Get(key, got); err != nil {
				return err
			}
			if !meta.IsStatusConditionFalse(
				got.Status.Conditions,
				multigresv1alpha1.ConditionUpToDate,
			) {
				return fmt.Errorf("UpToDate %+v", meta.FindStatusCondition(
					got.Status.Conditions, multigresv1alpha1.ConditionUpToDate))
			}
			return nil
		})
		c.awaitShardSettled(key, changedAt, settlePasses)
		current := &corev1.Pod{}
		c.NoError(c.Get(podKey, current), "get Lite pod")
		c.Eq(pod.UID, current.UID, "the Lite pod holding the PVC is kept")
		c.True(current.DeletionTimestamp.IsZero(), "and not being deleted")
		c.requirePVCTerminatingReported(key, dataName)
	})

	t.Run("recovers on a new PVC once the pod is deleted", func(t *testing.T) {
		c := c.Sub(t)
		current := &corev1.Pod{}
		c.NoError(c.Get(podKey, current), "get Lite pod")
		c.NoError(c.Delete(current), "delete the Lite pod")
		c.Eventually(30*time.Second, "a new data PVC and a new pod on it", func() error {
			pvc := &corev1.PersistentVolumeClaim{}
			if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: dataName}, pvc); err != nil {
				return err
			}
			if pvc.UID == original.UID {
				return fmt.Errorf("still the deleted PVC")
			}
			got := &corev1.Pod{}
			if err := c.Get(podKey, got); err != nil {
				return err
			}
			if got.UID == pod.UID || !podReady(got) {
				return fmt.Errorf("replacement pod not ready")
			}
			return nil
		})
		c.requirePVCTerminatingCleared(key)
	})
}

// clearsOrphanLabel reports whether patch is a merge patch removing the
// orphan label, which is what pvcutil.ClearOrphan sends.
func clearsOrphanLabel(patch client.Patch, obj client.Object) bool {
	if patch.Type() != types.MergePatchType {
		return false
	}
	data, err := patch.Data(obj)
	if err != nil {
		return false
	}
	var doc struct {
		Metadata struct {
			Labels map[string]*string `json:"labels"`
		} `json:"metadata"`
	}
	if json.Unmarshal(data, &doc) != nil {
		return false
	}
	v, ok := doc.Metadata.Labels[metadata.LabelOrphan]
	return ok && v == nil
}

func maps2slice(m map[string]types.UID) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// requirePVCProtected returns the named PVC and requires it to carry the
// pvc-protection finalizer, without which a deletion would remove it outright
// and these tests would not be exercising the state they claim to.
func (c *C) requirePVCProtected(name string) *corev1.PersistentVolumeClaim {
	c.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	c.NoError(c.Get(client.ObjectKey{Namespace: c.NS, Name: name}, pvc), "get PVC %s", name)
	if !slices.Contains(pvc.Finalizers, pvcProtectionFinalizer) {
		c.Fatalf("PVC %s has no %s finalizer; finalizers %v",
			name, pvcProtectionFinalizer, pvc.Finalizers)
	}
	return pvc
}

// releasePVCProtection releases the deleted PVC with the given UID, in a
// namespace whose test holds pvcProtectionSim, once nothing uses it, and waits
// for the deletion to finish.
func (c *C) releasePVCProtection(name string, uid types.UID) {
	c.Helper()
	c.Eventually(30*time.Second, "pvc-protection to release "+name, func() error {
		pvc := &corev1.PersistentVolumeClaim{}
		err := c.Get(client.ObjectKey{Namespace: c.NS, Name: name}, pvc)
		if apierrors.IsNotFound(err) || (err == nil && pvc.UID != uid) {
			return nil
		}
		if err != nil {
			return err
		}
		pods := &corev1.PodList{}
		if err := c.List(pods); err != nil {
			return err
		}
		if pvcInUse(pods.Items, name) {
			return fmt.Errorf("a pod still uses it")
		}
		if err := removePVCProtection(c.Context(), c.Client(), pvc); err != nil {
			return err
		}
		return fmt.Errorf("finalizer removed; waiting for the deletion to finish")
	})
}

// shardPodUIDs maps each of the shard's pool pods to its UID.
func (c *C) shardPodUIDs(key client.ObjectKey) map[string]types.UID {
	c.Helper()
	shard := &Shard{}
	c.NoError(c.Get(key, shard), "get shard")
	pods := &corev1.PodList{}
	c.NoError(c.List(pods, client.MatchingLabels{
		metadata.LabelMultigresShard:   string(shard.Spec.ShardName),
		metadata.LabelAppComponent:     shardcontroller.PoolComponentName,
		metadata.LabelMultigresCluster: shard.Labels[metadata.LabelMultigresCluster],
	}), "list pool pods")
	out := map[string]types.UID{}
	for _, p := range pods.Items {
		out[p.Name] = p.UID
	}
	return out
}

// deleteShardPods deletes the shard's pool pods, as a person would to accept
// losing a PVC that is being deleted.
func (c *C) deleteShardPods(key client.ObjectKey) {
	c.Helper()
	for name := range c.shardPodUIDs(key) {
		pod := &corev1.Pod{}
		err := c.Get(client.ObjectKey{Namespace: c.NS, Name: name}, pod)
		if apierrors.IsNotFound(err) {
			continue
		}
		c.NoError(err, "get pod %s", name)
		if err := c.Delete(pod); err != nil && !apierrors.IsNotFound(err) {
			c.Fatalf("delete pod %s: %v", name, err)
		}
	}
}

func (c *C) requirePVCTerminatingReported(key client.ObjectKey, pvcName string) {
	c.Helper()
	c.Eventually(15*time.Second, "the shard to report "+pvcName+" being deleted", func() error {
		shard := &Shard{}
		if err := c.Get(key, shard); err != nil {
			return err
		}
		cond := meta.FindStatusCondition(
			shard.Status.Conditions, multigresv1alpha1.ConditionPVCTerminating,
		)
		if cond == nil || cond.Status != "True" || !strings.Contains(cond.Message, pvcName) {
			return fmt.Errorf("condition %+v", cond)
		}
		if shard.Status.Phase != multigresv1alpha1.PhaseDegraded {
			return fmt.Errorf("phase %q", shard.Status.Phase)
		}
		return nil
	})
}

func (c *C) requirePVCTerminatingCleared(key client.ObjectKey) {
	c.Helper()
	c.Eventually(15*time.Second, "the PVCTerminating condition to clear", func() error {
		shard := &Shard{}
		if err := c.Get(key, shard); err != nil {
			return err
		}
		cond := meta.FindStatusCondition(
			shard.Status.Conditions, multigresv1alpha1.ConditionPVCTerminating,
		)
		if cond == nil || cond.Status != "False" {
			return fmt.Errorf("condition %+v", cond)
		}
		return nil
	})
}
