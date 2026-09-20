package suite

import (
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/testkit/ctrltest"
)

// TestTransitions is the round-trip suite: for a handful of optional
// MultigresCluster fields, set the field, then unset it, and require that the
// cluster has nothing left to do. No subtest enumerates what cleanup it
// expects to see; the closing assertion fails on any activity at all, whatever
// it is, which is what finds a leak without anyone having to name it first.
//
// That closing assertion takes one of two forms below, and the choice is per
// field rather than stylistic. Where the field's consequence lands on a plain
// corev1 kind whose event count is deterministic, it is a Script step
// permitting nothing but Quiet(). Where it does not, it is RequireQuiescent,
// which makes the same closed-world claim over the twelve kinds of
// watchedKinds() plus the write recorder rather than over the one or two kinds
// a Script could usefully watch, and which opens and closes its own watch
// instead of needing one opened before the fixture exists. A Script opened
// late over two kinds for a second is strictly weaker than the primitive it
// would be standing in for, so a field that cannot use the first form uses the
// second rather than a token version of the first.
//
// Each subtest also compares state directly across the round trip, which is
// not redundant with either form. An object created during the "set" and then
// left alone emits no event on the way back, so residue that manifests as the
// absence of an expected deletion is invisible to an event stream by
// construction, whichever kinds it watches.
//
// How far that comparison reaches differs per subtest, and none of them reach
// every kind. Backup compares PVC names and storage requests, DurabilityPolicy
// compares the TableGroup and Shard mirrors, and PVCDeletionPolicy compares its
// own field plus the namespace's PVC requests. Residue of a kind a subtest does
// not read still passes it: an orphaned ConfigMap or Service would pass all
// three.
//
// The field list came from api/v1alpha1/multigrescluster_types.go rather than
// from this task's brief, as instructed. Three of the four named fields exist
// as optional fields on MultigresClusterSpec and are covered below:
// PVCDeletionPolicy, Backup and DurabilityPolicy. The fourth, "a pool's
// Replicas", does not exist under that name: PoolSpec (shard_types.go) has no
// Replicas field, only ReplicasPerCell *int32. Per this task's brief, a field
// that does not exist under the given name is reported rather than silently
// substituted, so there is no fourth subtest here.
func TestTransitions(t *testing.T) {
	t.Run("PVCDeletionPolicy", testPVCDeletionPolicyRoundTrip)
	t.Run("Backup", testBackupRoundTrip)
	t.Run("DurabilityPolicy", testDurabilityPolicyRoundTrip)
}

// updateCluster applies mutate to a fresh read of the cluster and writes it
// back, failing the test rather than returning an error: a write that does not
// land is this file's own setup breaking, never an observation about the
// operator.
func (c *C) updateCluster(
	cluster *MultigresCluster,
	mutate func(*MultigresCluster),
) {
	c.Helper()
	got := &MultigresCluster{}
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), got), "get cluster")
	mutate(got)
	c.NoError(c.Update(got), "update cluster")
}

// pvcRequests reads every PVC in ns with the storage request it carries, for
// the snapshot-and-compare half of a round trip. Comparing the whole map
// catches a PVC that appeared, a PVC that went away and was never recreated,
// and a request that moved and stayed moved, none of which the event stream
// can report once the object stops changing.
func (c *C) pvcRequests() map[string]string {
	c.Helper()
	pvcs := &corev1.PersistentVolumeClaimList{}
	c.NoError(c.List(pvcs), "list PVCs")
	out := make(map[string]string, len(pvcs.Items))
	for _, pvc := range pvcs.Items {
		out[pvc.Name] = pvc.Spec.Resources.Requests.Storage().String()
	}
	return out
}

// shardBackupSizes reads the backup storage size each Shard has resolved. That
// is the value BuildSharedBackupPVC applies the shared backup PVC from
// (pool_pvc.go), so it is where a Backup write has to arrive for the PVC to
// see it.
func (c *C) shardBackupSizes() map[string]string {
	c.Helper()
	shards := &ShardList{}
	c.NoError(c.List(shards), "list Shards")
	if len(shards.Items) == 0 {
		c.Fatalf("no Shards in %s to read a resolved backup size from", c.NS)
	}
	out := make(map[string]string, len(shards.Items))
	for _, shard := range shards.Items {
		size := ""
		if shard.Spec.Backup != nil && shard.Spec.Backup.Filesystem != nil {
			size = shard.Spec.Backup.Filesystem.Storage.Size
		}
		out[shard.Name] = size
	}
	return out
}

// durabilityMirrors reads the DurabilityPolicy every TableGroup and Shard in
// ns currently carries. The field's only other consumer is the topology store,
// which this suite fakes in memory (fakes.go) and which writes a policy of its
// own regardless, so these mirrored spec fields are the whole of what this
// field does that anything here can observe.
func (c *C) durabilityMirrors() map[string]string {
	c.Helper()
	out := map[string]string{}
	tgs := &TableGroupList{}
	c.NoError(c.List(tgs), "list TableGroups")
	for _, tg := range tgs.Items {
		out["TableGroup/"+tg.Name] = tg.Spec.DurabilityPolicy
	}
	shards := &ShardList{}
	c.NoError(c.List(shards), "list Shards")
	for _, shard := range shards.Items {
		out["Shard/"+shard.Name] = shard.Spec.DurabilityPolicy
	}
	if len(out) == 0 {
		c.Fatalf("no TableGroups or Shards in %s to read a DurabilityPolicy from", c.NS)
	}
	return out
}

// testPVCDeletionPolicyRoundTrip watches only PersistentVolumeClaim.
//
// PVC is a plain corev1 kind with no status.conditions of its own, so writing
// to it never sets off the generation-bump status-condition churn that
// TableGroup and Shard produce on every spec write (see
// testDurabilityPolicyRoundTrip for where that churn made a Step-based
// assertion unusable). PVCDeletionPolicy's real consequence,
// reconcilePVCOwnerRefs (pkg/resource-handler/controller/shard/shard_controller.go),
// lands on PVCs directly, so this narrower watch still sees it, and nothing
// wider is needed to catch a leak here.
func testPVCDeletionPolicyRoundTrip(t *testing.T) {
	c := newCase(t)
	sc := c.NewScript(&corev1.PersistentVolumeClaimList{})
	sc.StepTimeout = 10 * time.Second

	cluster := c.MinimalCluster("pvcdp")
	c.WaitForClusterHealthy(cluster)

	// Snapshotted so the round trip is checked against the namespace's PVCs and
	// not only against the field's own value. A PVC created during the set and
	// then left alone emits nothing on the way back, so the closing Quiet step
	// cannot see it.
	pvcsAtStart := c.pvcRequests()
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	// The script opened before MinimalCluster, per NewScript's own contract,
	// so its watch replays every PVC the fixture created as an Added event
	// ("an already-populated namespace is replayed as a run of added
	// events"). Permitting that baseline explicitly, by listing what actually
	// exists now that convergence is independently confirmed, is the
	// documented way to handle it, and it is a statement about the fixture,
	// not a guess about this field's behaviour.
	pvcs := &corev1.PersistentVolumeClaimList{}
	c.NoError(c.List(pvcs), "list PVCs")
	c.NotEmpty(pvcs.Items, "MinimalCluster created no PVCs to test PVCDeletionPolicy against")
	baseline := make([]ctrltest.Allow, 0, len(pvcs.Items)*2)
	pvcNames := make([]string, 0, len(pvcs.Items))
	for _, pvc := range pvcs.Items {
		baseline = append(baseline,
			ctrltest.Added("PersistentVolumeClaim", pvc.Name),
			ctrltest.Changed("PersistentVolumeClaim", pvc.Name))
		pvcNames = append(pvcNames, pvc.Name)
	}
	sc.Step("PVCs created and bound during initial convergence", nil, baseline...)

	sc.Step("cluster settled", nil, ctrltest.Quiet())

	original := cluster.Spec.PVCDeletionPolicy.DeepCopy()

	consequences := make([]ctrltest.Allow, 0, len(pvcNames))
	for _, name := range pvcNames {
		consequences = append(consequences, ctrltest.Changed("PersistentVolumeClaim", name))
	}

	sc.Step("set PVCDeletionPolicy to Retain/Retain", func() error {
		got := &MultigresCluster{}
		if err := c.Get(client.ObjectKeyFromObject(cluster), got); err != nil {
			return err
		}
		got.Spec.PVCDeletionPolicy = &PVCDeletionPolicy{
			WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
			WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
		}
		return c.Update(got)
	}, consequences...)

	sc.Step("settled after setting the field", nil, ctrltest.Quiet())

	sc.Step("unset PVCDeletionPolicy", func() error {
		got := &MultigresCluster{}
		if err := c.Get(client.ObjectKeyFromObject(cluster), got); err != nil {
			return err
		}
		got.Spec.PVCDeletionPolicy = nil
		return c.Update(got)
	}, consequences...)

	sc.Step("nothing left to do after the round trip", nil, ctrltest.Quiet())

	sc.Finish(time.Second)

	// PVCDeletionPolicy carries a CRD-level +kubebuilder:default, so a nil
	// pointer never survives the API server: MinimalCluster's explicit
	// {Delete, Delete} and an omitted field both resolve to the same stored
	// value. The round trip is checked against what the field actually reads
	// back as, not against a literal nil.
	final := &MultigresCluster{}
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), final), "get final cluster")
	c.Check().EqDiff(original, final.Spec.PVCDeletionPolicy,
		"PVCDeletionPolicy round trip not lossless")
	c.Check().EqDiff(pvcsAtStart, c.pvcRequests(),
		"PVCs differ across the round trip")
}

// testBackupRoundTrip takes Backup out to an explicit backup storage size and
// back. It is the subtest that found a defect, and the pin below is the
// finding rather than an aside.
//
// Backup's only consequence any watch in this suite can see is the shared
// backup PVC's storage request (pool_pvc.go). The rest of what the field
// feeds is the topology store, faked in memory here (fakes.go), or needs a
// Secret the caller precreates (reconcile_shared_infra.go). So the round trip
// is driven through that size, and the size is where the operator breaks.
//
// Which size, measured against this harness rather than assumed, because once
// the PVC is bound and the data-plane fake has copied its request into
// status.capacity (datasim.go) every candidate is refused for a different
// reason:
//
//   - smaller, 5Gi against the resolver's 10Gi default, is refused by PVC
//     validation: "spec.resources.requests.storage: Forbidden: field can not
//     be less than status.capacity". That rule is unconditional in Kubernetes,
//     so any user of the operator can reach it.
//   - larger, 20Gi, is refused by the PersistentVolumeClaimResize admission
//     plugin: "only dynamically provisioned pvc can be resized and the
//     storageclass that provisions the pvc must support resize", because this
//     suite creates no StorageClass. A real cluster whose class sets
//     allowVolumeExpansion would accept it, so that refusal is an artifact of
//     the fixture and is deliberately not what gets pinned here.
//   - equal but written in another unit, 10240Mi, is canonicalised back to
//     10Gi by the API server and bumps no resourceVersion, so it is invisible
//     rather than illegal.
//
// The shrink is therefore the transition worth driving: legal on the
// MultigresCluster, propagated all the way to the PVC apply, and refused
// there.
func testBackupRoundTrip(t *testing.T) {
	c := newCase(t)
	ns := c.NS
	cluster := c.MinimalCluster("backup")
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	pvcsBefore := c.pvcRequests()
	sizesBefore := c.shardBackupSizes()
	original := cluster.Spec.Backup.DeepCopy()

	c.updateCluster(cluster, func(c *MultigresCluster) {
		c.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeFilesystem,
			Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
				Storage: multigresv1alpha1.StorageSpec{Size: "5Gi"},
			},
		}
	})

	// The defect: the refused PVC apply returns an error from Reconcile
	// (shard_controller.go), so everything after that block is skipped for as
	// long as the field stays lowered, the postgres ConfigMap render, the pool
	// pods, PDB sizing and reconcilePVCOwnerRefs, and controller-runtime
	// retries forever. The status update is NOT skipped: updateStatus runs
	// early in Reconcile, deliberately, so a wedged shard keeps reporting a
	// current observedGeneration and phase. That is what makes this defect
	// silent, and it is why a triage that looks for a stale status will not
	// find one. A user who lowers this field wedges
	// the shard's whole reconcile loop, with no clamping, no rejection at
	// admission, and no condition on the Shard saying why.
	//
	// Pinned as non-quiescence rather than as a missing PVC event, because a
	// missing event is what the API server's refusal guarantees on every run
	// forever: no operator change could ever retire that pin, and a pin that
	// cannot expire is a suppression. This one expires the day the operator
	// clamps the value, because then the namespace goes quiet and quiescent
	// returns nil.
	//
	// Two neighbouring fixes do not expire it through quiescence, and the
	// difference is worth knowing before trusting this pin as a tracker. An
	// admission refusal fails the test earlier, at the update call, so the
	// suite still goes red but by another route. A fix that gives up and
	// records a terminal condition only after retrying past this horizon's
	// activity budget leaves the pin green, so that one has to be noticed by a
	// human reading this comment rather than by the pin flipping.
	//
	// What this pin rests on, measured 2026-09-18 rather than assumed, because
	// it used to rest on something weaker than it looked. The wedged pass makes
	// several accepted no-op writes (both pg_hba and exporter-queries
	// ConfigMaps, the multiorch Deployment and Service, a Shard status patch)
	// before it reaches the refused PVC apply, so before the recorder counted
	// rejected writes, this pin observed non-quiescence only through those
	// earlier writes. Reordering updateStatus, a refactor with no behavioural
	// intent, would have flipped it to "appears fixed" with the defect fully
	// present. The recorder now counts the refused apply itself, which is the
	// one signal the defect cannot occur without, so that particular
	// reordering can no longer fool it.
	//
	// It is not yet true that the rejection alone carries the pin. Counting
	// only rejected writes, the same wedge measured non-quiescent on one run
	// and quiet on the next, 12 refused applies being right at the edge of a
	// 5s window inside an 11s horizon as the retry backoff spreads them out.
	// So the margin still comes from the signals combined. Widening the
	// horizon is not the fix (see below); if this pin ever needs to stand on
	// the rejection by itself, count refused reconcile passes directly rather
	// than inferring them from a quiet window.
	//
	// The horizon is short on purpose. controller-runtime retries a failing
	// Reconcile with exponential backoff, so the gap between failing passes
	// grows without bound and a long enough horizon would let the backoff
	// itself supply the quiet window while the shard is still wedged.
	// Calibrated both ways on this harness, three runs each: measured from
	// immediately after a legal spec write this goes quiet in about 5.3s,
	// measured from immediately after this write it never goes quiet inside
	// 11s, over 12 failing reconcile passes.
	c.KnownDefect("MGO-BACKUP-PVC-SHRINK-WEDGES-SHARD-RECONCILE", func() error {
		err := c.TryQuiescent(5*time.Second, 11*time.Second)
		// A lost watch voids the measurement instead of observing the
		// defect, and a non-nil error here is read as the defect still
		// being present, which would hold this pin green on an unrelated
		// failure.
		c.True(err == nil || !strings.Contains(err.Error(), "is void"),
			"quiescence over %s is void, so it is no evidence either way: %v", ns, err)
		return err
	})

	// Where the value actually got to, as an executable claim rather than a
	// comment, because the mechanism is easy to misread: it does reach the
	// Shard, so the PVC is applied from the size the user asked for and the
	// refusal happens at the API server. A fix aimed at the resolver's backup
	// defaulting would land on code that is behaving correctly.
	c.Eventually(15*time.Second, "the lowered size to reach every Shard", func() error {
		for name, size := range c.shardBackupSizes() {
			if size != "5Gi" {
				return fmt.Errorf("Shard %s resolved backup size is %q", name, size)
			}
		}
		return nil
	})

	c.updateCluster(cluster, func(c *MultigresCluster) {
		c.Spec.Backup = nil
	})

	// The gate that makes the closing assertion mean something. The namespace
	// is not quiet when the unset lands, so silence afterwards would be
	// ambiguous between the operator having processed it and the retry backoff
	// having merely grown past the window. The resolved size returning to
	// where it started is positive evidence that the unset propagated back
	// down to where the PVC is applied from. It is a wait, not the assertion.
	c.Eventually(30*time.Second, "the unset to reach every Shard", func() error {
		if got := c.shardBackupSizes(); !maps.Equal(got, sizesBefore) {
			return fmt.Errorf("resolved backup sizes are %v, want %v", got, sizesBefore)
		}
		return nil
	})

	// The round trip's assertion: once the cluster is back to the
	// configuration it started in, a converged operator has nothing left to
	// do, and this is that claim over every kind the operator writes plus
	// every write it makes.
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	c.Check().EqDiff(pvcsBefore, c.pvcRequests(), "PVCs did not round trip")
	final := &MultigresCluster{}
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), final), "get final cluster")
	c.Check().EqDiff(original, final.Spec.Backup, "Backup round trip not lossless")
}

// testDurabilityPolicyRoundTrip has no Script, and the reason is worth
// recording because the shape it would need does not exist in the runner.
//
// DurabilityPolicy is mirrored into TableGroup.Spec and Shard.Spec
// (builders_tablegroup.go, tablegroup/builders.go), and any spec write to
// either bumps its generation, which sends every controller watching it back
// to re-stamp its own status conditions' observedGeneration. That catch-up
// took a different number of passes on every run measured while writing this
// test: watching only TableGroup and Shard, the same single field write
// settled after 19, then 13, then 9 total events across three otherwise
// identical runs. A Step's allow-list is an exact multiset, with no "N events
// of this kind" wildcard available, so declaring one against a count that
// moves between runs would flake on this suite's own harness rather than on
// the operator, which is a worse failure than not writing the assertion at
// all.
//
// What is left for a Script to assert over those kinds is that nothing further
// happened, and RequireQuiescent asserts that strictly better: five seconds
// over twelve kinds plus the write recorder, rather than about a second over
// two, and it needs no watch opened before the fixture, which over these kinds
// is not possible to combine with an exact Step anyway. So the closing
// RequireQuiescent is this subtest's closed-world assertion, standing where
// the other form's Quiet() step stands.
func testDurabilityPolicyRoundTrip(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("durability")
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	original := cluster.Spec.DurabilityPolicy
	mirrorsBefore := c.durabilityMirrors()

	c.updateCluster(cluster, func(c *MultigresCluster) {
		c.Spec.DurabilityPolicy = "MULTI_CELL_AT_LEAST_2"
	})

	// Not an expectation about cleanup, which this test writes none of. It is
	// the guard that keeps the round trip from being vacuous: if setting the
	// field moved nothing anywhere, unsetting it could not leak anything and
	// the closing assertion would be proving nothing about this field.
	c.Eventually(30*time.Second, "the set policy to reach every mirror", func() error {
		for name, policy := range c.durabilityMirrors() {
			if policy != "MULTI_CELL_AT_LEAST_2" {
				return fmt.Errorf("%s carries %q", name, policy)
			}
		}
		return nil
	})
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	c.updateCluster(cluster, func(c *MultigresCluster) {
		c.Spec.DurabilityPolicy = original
	})

	// The mirrors returning is the state half of the round trip, and no event
	// assertion can make it: a mirror left holding the set value emits nothing
	// once it stops changing, so silence and correctness would be the same
	// observation. Also the gate that the operator processed the unset before
	// the assertion below asks for silence.
	c.Eventually(30*time.Second, "the unset policy to reach every mirror", func() error {
		if got := c.durabilityMirrors(); !maps.Equal(got, mirrorsBefore) {
			return fmt.Errorf("mirrors are %v, want %v", got, mirrorsBefore)
		}
		return nil
	})

	c.RequireQuiescent(5*time.Second, 30*time.Second)

	final := &MultigresCluster{}
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), final), "get final cluster")
	c.Check().Eq(original, final.Spec.DurabilityPolicy, "DurabilityPolicy round trip not lossless")
}
