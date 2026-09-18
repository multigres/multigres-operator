package suite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ctrltest"
	topopkg "github.com/multigres/multigres-operator/pkg/data-handler/topo"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// TestThrash adds and removes things faster than the operator can converge,
// then asserts it lands in the right end state and stops. Both subtests
// assert only two things: what the namespace looks like once it settles, and
// that it does settle at all.
//
// Neither subtest uses a Script. A Script's steps are a closed-world
// assertion of one interleaving of events, and thrash does not produce one
// interleaving: which reconciler wins a given race, how many redundant SSA
// applies land, and how many requeues fire along the way are all legitimately
// unpredictable once changes are issued faster than the operator can react to
// them. A Script step here would be asserting an ordering nobody can
// guarantee, which is exactly the kind of flake this suite exists to stop
// writing. So the claim is only about the end state and about quiescence,
// made with ordinary reads and RequireQuiescent, not about the path taken to
// get there. Do not "improve" this into a Script.
func TestThrash(t *testing.T) {
	t.Run("pool replica thrash", testPoolReplicaThrash)
	t.Run("child CR thrash", testChildCRThrash)
}

// thrashPoolName matches resolver.DefaultPoolName, the name the operator
// gives the pool it injects when a cluster specifies none. Importing
// pkg/resolver for one string constant did not seem worth the dependency, so
// this is a literal deliberately kept next to its cross-check.
const thrashPoolName = multigresv1alpha1.PoolName("default")

// testPoolReplicaThrash scales one pool 1 -> 2 -> 1 -> 2 with no wait between
// changes, then requires the namespace to go quiet and every pool PVC to be
// bound to a live pod.
//
// It does not itself hard-assert that this one thrashed shard reaches two
// members: see scaleUpRoleStaleAttempts below for why that particular claim
// is made statistically, across independent attempts, rather than against
// this one run.
func testPoolReplicaThrash(t *testing.T) {
	ns := Suite.Namespace(t)
	cluster := poolThrashCluster(t, ns, "pool-thrash", 1)
	waitForClusterHealthy(t, cluster)

	// Fired back to back, not waited on between calls: each Patch is an
	// unconditional merge patch computed against the object's state as of the
	// previous call in this loop, so it lands regardless of what the operator
	// has or hasn't done with the prior one yet. That is the thrash.
	for _, n := range []int32{2, 1, 2} {
		scalePoolTo(t, cluster, n)
	}

	// A live defect, found here and pinned below: once a shard has
	// reconciled to Healthy with N poolers, raising replicasPerCell to add
	// an (N+1)th pooler sometimes never gets that pooler's role into
	// shard.Status.PodRoles, which is what MembersOf and everything
	// downstream of it reads.
	ctrltest.KnownDefect(t, "MGO-POOL-SCALEUP-ROLE-STALE", func() error {
		return checkPoolScaleUpRoleStale(t)
	})

	Suite.RequireQuiescent(t, ns, 10*time.Second, 90*time.Second)

	// The end state, asserted on this shard rather than inferred from the pin's
	// four other namespaces. Pod readiness is a Kubernetes-level fact that does
	// not travel through shard.Status.PodRoles, so this is immune to the defect
	// pinned above: without it a lost or reverted final scale-up settles at one
	// pod, one bound PVC and a quiet namespace, and every other assertion here
	// is satisfied by that.
	live := liveReadyPoolPodNames(t, ns)
	if len(live) != 2 {
		t.Errorf("pool settled at %d ready pods, want 2: %v", len(live), live)
	}

	requireNoOrphanedPoolPVCs(t, ns, live)
}

// scaleUpRoleStaleAttempts is how many independent scale-up attempts
// checkPoolScaleUpRoleStale runs before concluding the defect is no longer
// reproducible.
//
// The defect is nondeterministic per attempt, so one attempt cannot pin it: a
// single check() racing the same coin flip it is meant to report on will
// flip between "present" and "appears fixed" from run to run with no code
// change in between (measured directly: five full runs of this test, before
// this comment existed, split 3 stuck / 2 converged). What is stable, also
// measured directly, is that a stuck attempt stays stuck: an isolated
// reproduction given a four-minute window never converged, so this is a
// permanent-until-something-external-changes failure per attempt, not a slow
// one. That is what makes N independent attempts a sound statistical claim
// rather than N re-rolls of the same question: each attempt is answered
// definitively (converged, or genuinely stuck within its own window), on its
// own namespace, so they are independent trials.
//
// Measured 2026-09-18, against this mechanism rather than an earlier one: 20
// attempts across five full-file runs, 10 stuck and 10 converged, so p is
// about 0.5. At six attempts, all converging by chance is 0.5^6, about 1.6%.
//
// Twenty trials is a thin sample and the interval is wide: the 95% Wilson
// bounds on 10/20 run roughly 30% to 70%, and at the favourable end of that
// six attempts give 0.7^6, about 12%.
//
// N was 4 until this pin false-expired once in six observations, which is what
// 0.5^4 = 6.25% predicts and what a green CI run cannot survive: the message a
// false expiry prints is "appears fixed; replace this pin", and acting on it
// deletes a pin for a live defect. If it happens again, the next move is not a
// seventh serial attempt, because each one costs a convergence wait and this
// subtest is already the slowest in the package. Run the attempts concurrently
// (they are already in separate namespaces), or drop the pin to a measurement
// that logs the stuck count and never claims an expiry, which is what this
// task's first author judged and which the arithmetic has now twice supported.
//
// An earlier version of this comment claimed 2.6%, from a sample of a
// different experiment: whole subtests failing after a full thrash, where this
// pin runs a bare single scale-up. The error was entirely in the unsafe
// direction, because assuming a higher failure rate makes the pin look less
// likely to expire by luck than it is. Re-measure against what the check
// actually does, not against whatever number is nearest.
//
// An earlier version of this comment claimed 2.6%, from a sample of a
// different experiment: whole subtests failing after a full thrash, where this
// pin runs a bare single scale-up. The error was entirely in the unsafe
// direction, because assuming a higher failure rate makes the pin look less
// likely to expire by luck than it is. Re-measure against what the check
// actually does, not against whatever number is nearest.
const scaleUpRoleStaleAttempts = 6

// checkPoolScaleUpRoleStale is the check body for
// KnownDefect(MGO-POOL-SCALEUP-ROLE-STALE). See scaleUpRoleStaleAttempts for
// why it runs more than one attempt and how many.
func checkPoolScaleUpRoleStale(t *testing.T) error {
	t.Helper()

	var failures []string
	for i := range scaleUpRoleStaleAttempts {
		if diagnosis, stuck := attemptPoolScaleUpRole(t, i); stuck {
			failures = append(failures, diagnosis)
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%d of %d independent pool scale-up attempts left the new pooler's role "+
			"missing from status.podRoles within the wait window:\n%s",
		len(failures), scaleUpRoleStaleAttempts, strings.Join(failures, "\n"),
	)
}

// attemptPoolScaleUpRole runs one independent 1 -> 2 pool scale-up on its own
// namespace (no thrash: a bare, single scale, so a stuck result cannot be
// blamed on interaction between the multiple patches testPoolReplicaThrash
// fires) and reports whether the new pooler's role landed in
// status.podRoles within the wait window.
//
// On a stuck attempt it also queries the topology store directly, bypassing
// the shard controller entirely, and reports the shard controller's own
// reconcile count over the same window (via Suite.Reconciles). That is the
// evidence that makes this a status-reporting defect rather than a topology
// or posture-evaluation one: the topology store already has the new pooler
// correctly registered, reporting RoutingRole REPLICA with no lifecycle
// shutdown/quarantine marker, while the shard controller keeps reconciling
// successfully (no errors) the entire time and never gets that fact into
// status.podRoles.
func attemptPoolScaleUpRole(t *testing.T, attempt int) (diagnosis string, stuck bool) {
	t.Helper()

	ns := Suite.Namespace(t)
	name := fmt.Sprintf("scaleup-%d", attempt)
	cluster := poolThrashCluster(t, ns, name, 1)
	waitForClusterHealthy(t, cluster)
	key := shardKey(t, ns)

	scalePoolTo(t, cluster, 2)

	deadline := time.Now().Add(45 * time.Second)
	var last Members
	for time.Now().Before(deadline) {
		members, err := MembersOf(t.Context(), Suite.Client, key)
		if err == nil {
			last = members
			if len(members.Replicas) == 1 && len(members.Quarantined) == 0 {
				return "", false
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	diag := fmt.Sprintf(
		"attempt %d (namespace %s): members stuck at %+v 45s after scaling to 2",
		attempt, ns, last,
	)

	shard := &multigresv1alpha1.Shard{}
	if err := Suite.Client.Get(t.Context(), key, shard); err == nil {
		store := topo.Store(ns)
		poolers, err := store.GetMultipoolersByCell(
			t.Context(), string(defaultSimCell), topopkg.ShardFilter(shard),
		)
		if err == nil {
			var roles []string
			for _, p := range poolers {
				roles = append(roles, fmt.Sprintf(
					"%s routingRole=%s lifecycle=%v",
					p.GetHostname(), p.GetRoutingState().GetRole(), p.GetLifecycleStatus(),
				))
			}
			diag += fmt.Sprintf("; topology store directly reports: %v", roles)
		}
	}

	var reconciles, errs int
	for _, r := range Suite.Reconciles.InNamespace(ns) {
		if r.Controller == "shard" {
			reconciles++
			if r.Err != nil {
				errs++
			}
		}
	}
	diag += fmt.Sprintf("; shard controller reconciled %d time(s) (%d error(s)) meanwhile",
		reconciles, errs)

	return diag, true
}

// poolThrashCluster creates a MultigresCluster with an explicit
// spec.databases entry, rather than relying on MinimalCluster's implicit
// "zero config" defaulting.
//
// TableGroup and Shard pool specs are fully owned by the cluster controller's
// server-side apply, recomputed from spec.databases fresh on every reconcile
// (PopulateClusterDefaults runs in-memory only; nothing persists the
// zero-config defaults back to the MultigresCluster object). So a pool's
// replica count can only be changed durably by writing spec.databases: a
// direct edit to TableGroup.Spec or Shard.Spec.Pools would be reverted by the
// very next MultigresCluster reconcile, which Owns(&TableGroup{}) and fires
// on exactly that edit.
//
// A single cell with the zero-config default pool would resolve to
// replicasPerCell=2 already (ResolveShard defaults a single-cell pool to 2,
// the minimum the AT_LEAST_2 durability policy needs), which leaves no room
// to scale down to 1 first. Declaring the pool explicitly, starting at
// replicasPerCell, sidesteps that and lets the thrash sequence start at 1.
func poolThrashCluster(
	t *testing.T,
	ns, name string,
	replicasPerCell int32,
) *multigresv1alpha1.MultigresCluster {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: adminSecretName, Namespace: ns},
		StringData: map[string]string{"password": "postgres"},
	}
	if err := Suite.Client.Create(t.Context(), secret); err != nil {
		t.Fatalf("create password secret: %v", err)
	}

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: adminSecretName,
				Key:  "password",
			},
			PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
			Cells: []multigresv1alpha1.CellConfig{
				{Name: defaultSimCell, ZoneID: "us-central1-a"},
			},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name:    "postgres",
				Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name:    "default",
					Default: true,
					Shards: []multigresv1alpha1.ShardConfig{{
						Name: "0-inf",
						Spec: &multigresv1alpha1.ShardInlineSpec{
							Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
								thrashPoolName: poolSpecWithReplicas(replicasPerCell),
							},
						},
					}},
				}},
			}},
		},
	}
	if err := Suite.Client.Create(t.Context(), cluster); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	return cluster
}

func poolSpecWithReplicas(n int32) multigresv1alpha1.PoolSpec {
	return multigresv1alpha1.PoolSpec{
		Type:            "readWrite",
		Cells:           []multigresv1alpha1.CellName{defaultSimCell},
		ReplicasPerCell: ptr.To(n),
	}
}

// scalePoolTo patches cluster's pool to n replicas per cell via a merge patch
// against cluster's own in-memory state, not a fresh read of the server. That
// makes each call in a back-to-back thrash loop independent of whatever the
// operator has done with the previous one: the patch always states the full
// desired pool spec, so it lands regardless of the server's current state.
func scalePoolTo(t *testing.T, cluster *multigresv1alpha1.MultigresCluster, n int32) {
	t.Helper()
	base := cluster.DeepCopy()
	pools := cluster.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools
	pools[thrashPoolName] = poolSpecWithReplicas(n)
	if err := Suite.Client.Patch(t.Context(), cluster, client.MergeFrom(base)); err != nil {
		t.Fatalf("scale pool %s to %d replicas per cell: %v", thrashPoolName, n, err)
	}
}

// liveReadyPoolPodNames lists Ready pods belonging to the thrashed pool.
//
// This deliberately does not go through MembersOf/shard.Status.PodRoles: that
// path is exactly what MGO-POOL-SCALEUP-ROLE-STALE (see
// testPoolReplicaThrash) breaks, and a pod being live is a Kubernetes-level
// fact independent of whether the operator's own role bookkeeping has caught
// up to it.
func liveReadyPoolPodNames(t *testing.T, ns string) []string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := Suite.Client.List(
		t.Context(), pods, client.InNamespace(ns),
		client.MatchingLabels{metadata.LabelMultigresPool: string(thrashPoolName)},
	); err != nil {
		t.Fatalf("list pool pods: %v", err)
	}
	var names []string
	for i := range pods.Items {
		if podReady(&pods.Items[i]) {
			names = append(names, pods.Items[i].Name)
		}
	}
	return names
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// requireNoOrphanedPoolPVCs asserts every PVC belonging to the thrashed pool
// is bound to one of livePods. It resolves the binding with ShardPVCOf, reading it
// off each live pod's own volumes, rather than reconstructing a PVC name from
// the pool/cell/ordinal and asserting the two strings match: a pod bound to
// the wrong PVC would pass a name-arithmetic check and fail this one.
func requireNoOrphanedPoolPVCs(t *testing.T, ns string, livePods []string) {
	t.Helper()

	bound := map[string]bool{}
	for _, pod := range livePods {
		pvcName, err := ShardPVCOf(t.Context(), Suite.Client, ns, pod)
		if err != nil {
			t.Fatalf("resolve PVC bound to live pod %s: %v", pod, err)
		}
		bound[pvcName] = true
	}

	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := Suite.Client.List(
		t.Context(), pvcs, client.InNamespace(ns),
		client.MatchingLabels{metadata.LabelMultigresPool: string(thrashPoolName)},
	); err != nil {
		t.Fatalf("list pool PVCs: %v", err)
	}
	for _, pvc := range pvcs.Items {
		if !bound[pvc.Name] {
			t.Errorf(
				"PVC %s belongs to pool %s but is not bound to any live pod; live pods: %v",
				pvc.Name, thrashPoolName, livePods,
			)
		}
	}
}

// testChildCRThrash deletes a Shard out from under its TableGroup and lets
// the parent recreate it, three times in a row, then requires the shard to
// reconverge and the namespace to go quiet.
//
// This is the likeliest spot in the wave to find a live defect: the
// ReadyForDeletion protocol between the shard and tablegroup controllers is
// already the subject of two filed defects (a vacuous ReadyForDeletion, and
// whole-cluster teardown skipping the drain), and it found a third here, pinned
// below.
func testChildCRThrash(t *testing.T) {
	ns := Suite.Namespace(t)
	cluster := MinimalCluster(t, ns, "cr-thrash")
	waitForClusterHealthy(t, cluster)

	key := shardKey(t, ns)
	shard := &multigresv1alpha1.Shard{}

	for i := 0; i < 3; i++ {
		if err := Suite.Client.Get(t.Context(), key, shard); err != nil {
			t.Fatalf("cycle %d: get shard %s: %v", i, key.Name, err)
		}
		oldUID := shard.UID
		if err := Suite.Client.Delete(t.Context(), shard); err != nil {
			t.Fatalf("cycle %d: delete shard %s: %v", i, key.Name, err)
		}

		// The only wait in this loop: for the parent to have recreated a
		// replacement (a new UID at the same name), which is a mechanical
		// precondition for the next delete to hit a live object rather than a
		// no-op against one already gone. It is not a wait for the replacement
		// to converge, and the loop does not wait for that before deleting
		// again: that is the thrash.
		what := fmt.Sprintf("the tablegroup to recreate Shard %s after delete #%d", key.Name, i+1)
		ctrltest.Eventually(t, 30*time.Second, what, func() error {
			got := &multigresv1alpha1.Shard{}
			if err := Suite.Client.Get(t.Context(), key, got); err != nil {
				return err
			}
			if got.UID == oldUID {
				return fmt.Errorf("shard %s not yet recreated", key.Name)
			}
			return nil
		})
	}

	if err := Suite.Client.Get(t.Context(), key, shard); err != nil {
		t.Fatalf("get final shard incarnation: %v", err)
	}

	waitForClusterHealthy(t, cluster)

	ctrltest.Eventually(t, 60*time.Second, "the shard to report one primary and one replica",
		func() error {
			members, err := MembersOf(t.Context(), Suite.Client, key)
			if err != nil {
				return err
			}
			if len(members.Replicas) != 1 || len(members.Quarantined) != 0 {
				return fmt.Errorf(
					"want 1 primary + 1 replica + 0 quarantined, got %+v", members,
				)
			}
			return nil
		},
	)

	Suite.RequireQuiescent(t, ns, 10*time.Second, 90*time.Second)

	// A second live defect, found by this test: the shared backup PVC never
	// has its orphan label cleared when a torn-down Shard's replacement
	// reclaims it.
	//
	// reconcileSharedBackupPVC (reconcile_shared_infra.go) reapplies the PVC by
	// server-side apply from BuildSharedBackupPVC's payload, which never
	// mentions multigres.com/orphan-since, so SSA leaves that label exactly as
	// cleanupShardPVCs (reconcile_deletion.go) left it during the prior
	// teardown: marked orphan. Contrast the per-pool data PVC path, which
	// explicitly calls pvcutil.ClearOrphan on reuse
	// (reconcile_pool_pods.go:204). The shared backup PVC has no equivalent
	// call anywhere in the shard controller.
	//
	// Net effect: after any teardown-and-recreate of a Shard whose backup PVC
	// survives (WhenDeleted=Delete, which MinimalCluster sets, still only
	// orphans rather than deletes it in-line, because resolvePodIndex cannot
	// parse an ordinal out of a backup PVC's name-hash suffix and the !hasIndex
	// arm short-circuits before pvcOrphanReplicasThreshold is consulted at
	// all), the backup PVC is left labeled orphan
	// forever, even though it is immediately reclaimed and stays in active use
	// by the reconverged, healthy shard. The multigres-gc CronJob acts on
	// exactly that label, so in a real cluster this is a live backup volume
	// scheduled for deletion out from under a running shard.
	backupPVCKey := client.ObjectKey{
		Namespace: ns,
		Name:      shardcontroller.BuildSharedBackupPVCName(shard),
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := Suite.Client.Get(t.Context(), backupPVCKey, pvc); err != nil {
		t.Fatalf("get shared backup PVC %s: %v", backupPVCKey.Name, err)
	}
	ctrltest.KnownDefect(t, "MGO-BACKUP-PVC-ORPHAN-STALE", func() error {
		since, stale := pvc.Labels[metadata.LabelOrphan]
		if !stale {
			return nil
		}
		return fmt.Errorf(
			"shared backup PVC %s still carries %s=%s from an earlier teardown, though the "+
				"shard that owns it (uid %s) has reconverged healthy: reconcileSharedBackupPVC's "+
				"server-side apply never clears the label on reuse, unlike the per-pool data PVC "+
				"path (pvcutil.ClearOrphan in reconcile_pool_pods.go)",
			backupPVCKey.Name, metadata.LabelOrphan, since, shard.UID,
		)
	})
}

func waitForClusterHealthy(t *testing.T, cluster *multigresv1alpha1.MultigresCluster) {
	t.Helper()
	ctrltest.Eventually(t, 30*time.Second, "cluster to report Healthy", func() error {
		got := &multigresv1alpha1.MultigresCluster{}
		if err := Suite.Client.Get(
			t.Context(), client.ObjectKeyFromObject(cluster), got,
		); err != nil {
			return err
		}
		if got.Status.Phase != multigresv1alpha1.PhaseHealthy {
			return fmt.Errorf("phase is %q", got.Status.Phase)
		}
		return nil
	})
}
