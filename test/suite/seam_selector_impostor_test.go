// Selector inventory for the multigrescluster and shard controllers: every
// List call in pkg/cluster-handler/controller/multigrescluster/ and
// pkg/resource-handler/controller/shard/ that carries a label selector, the
// selector itself, and what this sweep found or judged about it.
//
// 35 List call sites in the two packages, 12 in multigrescluster and 23 in
// shard, of which 32 carry a label selector; the three that do not are
// recorded below as out of scope rather than omitted, so that the count can
// be rederived from this block. Line numbers point at the List( call, not at
// the MatchingLabels argument. Cross-checked for the other spellings a
// selector can take (MatchingLabelsSelector, HasLabels, a raw
// ListOptions{LabelSelector}): neither package uses any of them.
//
// multigrescluster (pkg/cluster-handler/controller/multigrescluster/):
//
//   - reconcile_cells.go:23        CellList        {cluster}
//     Feeds the active/orphan diff for cells removed from spec. The eventual
//     delete is gated behind the AnnotationPendingDeletion + ConditionReady-
//     ForDeletion handshake (reconcile_cells.go:92-125), not a raw sweep.
//     SAFE (condition-gated). Not tested here.
//
//   - reconcile_databases.go:23    TableGroupList  {cluster}
//     Same shape as reconcile_cells.go:23, for TableGroups. SAFE (condition-
//     gated); the protocol itself is already covered by
//     TestReadyForDeletionProtocol in seam_deletion_test.go.
//
//   - reconcile_topology.go:245    CellList        {cluster}
//     Read-only: collects names of cells pending deletion, for topology
//     pruning. Never mutates or deletes anything itself. SAFE.
//
//   - reconcile_global.go:113      TopoServerList  {cluster}
//     CONFIRMED LIVE DEFECT (Defect 2 in
//     tasks/multigres-operator-bugs-found-by-the-suite.md). No component
//     filter, no owner-reference check: when global topology is external,
//     every TopoServer carrying the cluster label is deleted, including a
//     cell's own local TopoServer (component "local-topo"), which this
//     selector cannot tell apart from the managed global one (component
//     "global-topo"). TESTED below:
//     TestSelectorImpostorGlobalTopoPruneDeletesCellOwnedLocalTopoServer.
//
//   - multigrescluster_controller.go:401  CellList        {cluster}, in
//     handleDeletion (whole-cluster teardown). Raw delete, no owner-ref
//     check. Reachable only while the cluster itself is being deleted, and
//     an impostor sharing the label would first be picked up by
//     reconcile_cells.go's own condition-gated path above (the cell
//     controller reconciles any Cell object regardless of who owns it),
//     which confounds an isolated impostor test for this exact line.
//     JUDGED UNREACHABLE IN ISOLATION within this pass; not tested. See the
//     task report for the reasoning in full.
//
//   - multigrescluster_controller.go:419  TableGroupList  {cluster}, in
//     handleDeletion. Same shape and same confound as line 401, via
//     reconcile_databases.go:23's condition-gated path. Not tested.
//
//   - multigrescluster_controller.go:439  PersistentVolumeClaimList
//     {cluster, component=toposerver}, in handleDeletion. Raw delete, no
//     owner-ref check, but the function's own comment states the intent:
//     these PVCs "may outlive their TopoServer when a cluster switches from
//     managed to external topology," i.e. an unowned PVC with this label
//     pair is the expected steady state this code exists to clean up, not
//     an anomaly. SAFE BY DESIGN for this task's "should do nothing to an
//     object it does not own" heuristic, because the whole point here is
//     that ownership cannot be established for what it is meant to sweep.
//     Not pinned as a defect; the design's blast radius (anything bearing
//     these two labels, from any source, is eligible) is flagged in the
//     report rather than pinned as a KnownDefect.
//
//     This is SAFE while shard_controller.go:589 below is a DEFECT on what
//     looks like the same evidence, an unowned PVC being acted on, and the
//     two verdicts are worth reading together rather than one at a time.
//     The difference is documented intent, which is the only thing that can
//     separate them: :439 says an unowned PVC carrying these labels is
//     precisely what it exists to sweep, whereas :589's own doc comment
//     (shard_controller.go:571-574) and its call site
//     (shard_controller.go:425-426) both say it exists to fix up ownerRefs
//     on the shard's own PVCs across a mid-lifecycle policy change. Acting
//     on a stranger is the job in one and an accident in the other.
//
//   - multigrescluster_controller.go:603  MultigresClusterList, InNamespace
//     only. Not a label selector (a map function for CoreTemplate/
//     CellTemplate/ShardTemplate change fanout). Out of scope.
//
//   - certificate.go:394 (via pkg/util/certs.List)  no label selector,
//     InNamespace only. The eventual delete (certs.Prune) checks
//     OwnedBy(cert, ownerUID) before deleting anything: the one place in
//     this controller that already does what reconcile_global.go:113 does
//     not. SAFE, and the contrast is the original bug write-up's own point.
//
//   - status.go:125, :150, :220  CellList / TableGroupList / TopoServerList
//     {cluster}. Purely read, to aggregate MultigresCluster.Status; nothing
//     is ever mutated or deleted at these call sites. SAFE for this task's
//     "does the controller act on it" question. A mislabelled object here
//     would only ever skew the cluster's own reported status, a different
//     risk this task's Quiet()-shaped assertion cannot express and which is
//     not assessed here.
//
// shard (pkg/resource-handler/controller/shard/):
//
//   - shard_controller.go:589 (reconcilePVCOwnerRefs)  PersistentVolumeClaimList
//     {cluster, database, tablegroup, shard} (no pool, no component). NEW
//     DEFECT found by this sweep: the selector is the shard's four identity
//     keys and nothing else, so it cannot tell the shard's own PVC from any
//     other object carrying the same identity, and the only test applied to
//     a match before adoption is whether it already carries a ref with this
//     shard's UID (shard_controller.go:625-631). A PVC belonging to nobody
//     fails that test, so it is adopted: SetControllerReference + Patch,
//     whenever the shard's effective PVCDeletionPolicy resolves to Delete.
//
//     There is a real ownership check here, and naming it correctly matters
//     because it bounds the defect. ctrl.SetControllerReference returns
//     AlreadyOwnedError when the object already carries a different
//     controller ownerRef (controller-runtime v0.25.0,
//     pkg/controller/controllerutil/controllerutil.go:97-99), so this code
//     does not adopt a PVC that already belongs to someone else. What it
//     adopts is a PVC with no controller owner at all. TESTED below:
//     TestSelectorImpostorShardOwnerRefReconcileAdoptsUnrelatedPVC.
//
//   - reconcile_deletion.go:57   DeploymentList  {cluster, database,
//     tablegroup, shard}, in the Shard's own handleDeletion. Raw delete, no
//     owner-ref check. Same family as shard_controller.go:589. Not
//     independently tested given this pass's budget.
//
//   - reconcile_deletion.go:90   PodList  same 4-key selector, same
//     function. Raw delete, no owner-ref check. Lower interference risk
//     than the multigrescluster Cell/TableGroup case above, since a Shard's
//     own teardown is not cascaded through another controller's graceful
//     orphan protocol. Not tested here.
//
//   - reconcile_deletion.go:166 (cleanupShardPVCs)  PersistentVolumeClaimList
//     same 4-key selector. Every match is marked orphan or deleted with no
//     owner-ref check, gated only by shardPVCShouldBeCleaned's policy read.
//     Same family as shard_controller.go:589 at a different lifecycle
//     point. Not independently tested.
//
//   - reconcile_deletion.go:252 (handlePendingDeletion)  PodList  same
//     4-key selector. Runs the drain state machine (initiateDrain /
//     clearDrainAnnotations / Delete) against any match once the Shard
//     itself carries the PendingDeletion annotation. Same family; combining
//     it with a graceful shard-level orphan flow adds the same entanglement
//     seen in the multigrescluster Cell/TableGroup case. Not tested.
//
//   - reconcile_data_plane.go:290, :367   PodList  same 4-key selector.
//     Read-only relative to the pods themselves (feeds
//     shard.Status.PodRoles and posture.Evaluate). SAFE.
//
//   - reconcile_data_plane.go:542 (reconcileDrainState)  PodList  same
//     4-key selector. MUTATES a matching pod (clears its drain annotations)
//     when isDrainStale holds, which requires the pod's pool label to
//     resolve to a real shard.Spec.Pools entry, a name that parses to an
//     in-range replica ordinal, and a spec judged unchanged from desired.
//     A real candidate, but reproducing that combination on a synthetic
//     impostor is disproportionate for this pass; deferred.
//
//   - reconcile_data_plane.go:673 (reconcilePoolerPrune)  PodList  same
//     4-key selector. The action it drives (topo.MarkDeadPoolers) writes to
//     the fake topology store, not to the Kubernetes object, so this
//     suite's k8s-event Stream cannot observe the outcome either way.
//     UNTESTABLE WITH THIS HARNESS.
//
//   - reconcile_quarantine.go:83 (reconcileQuarantineRemediation)  PodList
//     same 4-key selector. Deletes a pod and hard-deletes its PVC, but only
//     for names the fake topology store reports as LIFECYCLE_QUARANTINED.
//     Driving that requires reaching into the suite's internal topology
//     registry; deferred.
//
//   - disruption.go:30   PodList  {cluster, database, tablegroup, shard,
//     component=Pool} (adds the component key the multigrescluster prune
//     lacks). Read-only (feeds canStartDisruption's decision). SAFE.
//
//   - maintenance_surge.go:300, :338   PodList  component+cell-scoped via
//     shardPDBLabels/metadata.GetSelectorLabels. Read-only. SAFE.
//
//   - postgres_config.go:266   ShardList, InNamespace only. Not a label
//     selector (map function for ConfigMap-change fanout). Out of scope.
//
//   - reconcile_shared_infra.go:373   PodList  the PDB's own
//     component+pool+cell selector. Read-only (sizes MinAvailable). SAFE.
//
//   - reconcile_shared_infra.go:411   PodDisruptionBudgetList  same PDB
//     selector. Deletes an unmatched PDB, but only those that also pass
//     metav1.IsControlledBy(pdb, shard): an explicit owner check right in
//     the loop. SAFE, and the "done right" counterpart to
//     shard_controller.go:589.
//
//   - reconcile_pool_pods.go:50   PodList  pool+cell-scoped
//     (buildPoolLabelsWithCell). DEFECT of the same class as
//     shard_controller.go:589, and the strongest untested candidate left in
//     this inventory. The list populates existingPods
//     (reconcile_pool_pods.go:69-73), which is then walked by name with no
//     ownership check at all:
//
//     Phase 0, syncDrainedLabels (:82, body :943-:974), iterates every map
//     member and patches multigres.com/pod-role whenever
//     resolvePodRole(shard, pod.Name) disagrees with the label the pod
//     carries, so a pod this shard does not own has that label written or
//     stripped. Phase 2, handleScaleDown (:118, body :499), classifies any
//     member whose name does not parse as <prefix>-<int> (resolvePodIndex,
//     :1240-:1250) or whose index is at or beyond effectiveReplicas as an
//     extra pod (:537-:540), then drains (initiateDrain, :650) and deletes
//     it (:567). A plausible impostor Pod carrying the pool and cell labels
//     and any non-numeric name suffix is therefore drained and deleted.
//     Secondary consequence at the same site: isPoolHealthy(existingPods,
//     ...) (:585) counts an impostor as a pool member, so a non-ready one
//     blocks legitimate scale-down of the real pool.
//
//     NOT PINNED in this pass, and recorded here rather than left implied:
//     a pin is a test, and this one needs a Pod-kind impostor against
//     DataPlaneSim.tickPods, whose write set differs from tickPVCs's and
//     has not been derived (see the TestSelectorImpostorShardOwnerRef...
//     caveat below). Naming it SAFE, as an earlier revision of this block
//     did, was the error worth correcting: an unexamined site is a gap, and
//     a gap signed SAFE is worse than one left open.
//
//   - reconcile_pool_pods.go:60   PersistentVolumeClaimList  pool+cell-
//     scoped. SAFE, but name-keyed rather than read-only, which is the
//     accurate justification: pvcutil.ClearOrphan (:204) and
//     expandPVCIfNeeded (:216) both write to list members, and what makes
//     them safe is that every access is existingPVCs[pvcName] where
//     pvcName comes from BuildPoolDataPVCName, a deterministic desired
//     name. An impostor under any other name is never indexed, so it is
//     genuinely untouched. Unlike :50 above, which walks the map itself.
//
//   - reconcile_pool_pods.go:1116   PersistentVolumeClaimList  pool+cell-
//     scoped, counts non-orphan PVCs. Read-only. SAFE.
//
//   - reload.go:69   PodList  {cluster, database, tablegroup, shard,
//     component=Pool}. Read-only (feeds the reload decision). SAFE.
//
//   - status.go:221   PodList  pool+cell-scoped (buildPoolLabelsWithCell).
//     Read-only (status aggregation). SAFE.
//
//   - status.go:365   PodList  buildMultiorchLabelsWithCell selector.
//     Read-only (crash-loop detection for status). SAFE.
//
//   - reconcile_readiness.go:33   PodList  {cluster, database, tablegroup,
//     shard, component=Pool}. Read-only (readiness aggregation). SAFE.

package suite

import (
	"errors"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ctrltest"
	topopkg "github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// errLocalTopoDeleted is what the TopoServer script's invariant returns when
// it sees the deletion that test pins.
//
// A sentinel rather than a match on the harness's message text, because
// KnownDefect reads any non-nil error as "the pinned defect is still live".
// This pin's script can produce several other errors that are not that
// deletion, and every one of them would otherwise keep the pin green: a
// legitimate toposerver status write that the step's declaration failed to
// account for, a step that timed out because that declaration has drifted from
// what the controller now writes. Those are facts about this test, not about
// the operator, so the check body has to be able to tell them apart, and it
// cannot do that by reading a string the harness is free to reformat.
var errLocalTopoDeleted = errors.New(
	"the cell's own local TopoServer was deleted",
)

// selectorImpostorNudgeAnnotation is a key no controller's applied payload
// ever mentions, following the same reasoning as shardProbeAnnotation in
// seam_race_test.go: tablegroup's BuildShard sets an annotation map on a Shard
// only when its TableGroup carries a project-ref annotation
// (pkg/cluster-handler/controller/tablegroup/builders.go:37-48), which
// MinimalCluster's fixture never does, so writing this key is a mutation
// neither manager's SSA apply contends with or reverts.
//
// It has to enqueue the Shard to be useful, and it does: the shard
// controller's For(&multigresv1alpha1.Shard{}) (shard_controller.go:693)
// carries no predicate, so a metadata-only patch is a reconcile trigger. That
// is the whole reason the key exists, since a converged Shard has nothing left
// to re-trigger it and reconcilePVCOwnerRefs only looks at an impostor on a
// pass that actually runs.
const selectorImpostorNudgeAnnotation = "seam-selector-impostor-test.multigres.com/nudge"

// externalGlobalTopoCluster creates a MultigresCluster whose global topology is
// external and whose one cell manages its own local TopoServer, the exact
// combination tasks/multigres-operator-external-topo-deletes-local.md
// reproduced on a live cluster: it is what makes reconcileGlobalTopoServer's
// desired-is-nil branch run on every reconcile, while still giving the cell
// controller a local TopoServer of its own to keep reapplying.
//
// It returns an error rather than calling t.Fatalf as MinimalCluster does,
// because its caller runs it inside a Script step's do: a Fatalf there would
// Goexit out of the middle of a script, whereas Script.TryStep routes a failing
// do to the test's own Fatalf and, crucially, never lets it reach KnownDefect
// as though it were evidence about the operator.
//
// The password Secret is created here rather than shared with MinimalCluster
// because the two fixtures differ in every other field; what is worth keeping
// in step is the deliberate choice to leave it unlabelled, which is what a user
// would create and what makes the reconcilers' APIReader necessary.
func externalGlobalTopoCluster(t *testing.T, ns, clusterName string) error {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: adminSecretName, Namespace: ns},
		StringData: map[string]string{"password": "postgres"},
	}
	if err := Suite.Client.Create(t.Context(), secret); err != nil {
		return fmt.Errorf("create password secret: %w", err)
	}

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: adminSecretName,
				Key:  "password",
			},
			PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{
						"https://external-topo.invalid:2379",
					},
				},
			},
			Cells: []multigresv1alpha1.CellConfig{
				{
					Name:   defaultSimCell,
					ZoneID: "us-central1-a",
					Spec: &multigresv1alpha1.CellInlineSpec{
						LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
							Etcd: &multigresv1alpha1.EtcdSpec{
								Replicas: ptr.To(int32(1)),
							},
						},
					},
				},
			},
		},
	}
	if err := Suite.Client.Create(t.Context(), cluster); err != nil {
		return fmt.Errorf("create MultigresCluster: %w", err)
	}
	return nil
}

// TestSelectorImpostorGlobalTopoPruneDeletesCellOwnedLocalTopoServer pins
// Defect 2 from tasks/multigres-operator-bugs-found-by-the-suite.md:
// reconcile_global.go:113 lists TopoServers by cluster label alone (no
// component filter, no owner-reference check) and deletes every match
// whenever global topology is external. A cell's own local TopoServer
// carries that same cluster label, so it is not this controller's to
// manage, but the selector cannot tell the difference.
//
// The impostor here is not synthetic: it is the real local TopoServer the cell
// controller legitimately creates and keeps reapplying, which is exactly what
// makes the write-up call this "a permanent create/delete loop" rather than a
// one-off. That permanence is also what shapes the script below, because it
// rules out the move every other test in this suite makes first. An object
// caught in a create/delete loop never settles, so there is no converged
// namespace to open a watch onto: neither RequireQuiescent nor a poll for a
// stable TopoServer can be used here, and waiting a fixed margin for the
// toposerver controller to stop writing is a guess at how long another actor's
// work takes, which is the thing this suite exists to refuse.
//
// So the watch opens first, on an empty namespace, and the fixture is created
// inside the script's own step. Every legitimate write the toposerver
// controller then makes to the object is named as a permitted change, which
// leaves the deletion as the one event nothing accounts for, and leaves nothing
// to wait out.
func TestSelectorImpostorGlobalTopoPruneDeletesCellOwnedLocalTopoServer(t *testing.T) {
	ns := Suite.Namespace(t)
	const clusterName = "ext-global-topo"
	cellResourceName := name.JoinWithConstraints(
		name.DefaultConstraints, clusterName, string(defaultSimCell),
	)
	localTopoName := topopkg.ManagedLocalTopoServerName(cellResourceName)

	script := Suite.NewScript(t, ns, &multigresv1alpha1.TopoServerList{})

	// The deletion is this test's entire claim, so it is asserted directly
	// rather than inferred from being whatever event no step happened to
	// permit. Script.TryStep and Script.TryFinish both run the invariants
	// against an event before comparing it to the permitted set
	// (script.go:224, :243, :294), so the delete is reported as this violation
	// wherever it lands: while the step is still waiting on a status write,
	// inside its settle window, or inside Finish's horizon. Combined with the
	// sentinel above, that is what makes the pin's evidence the deletion on
	// every run instead of whichever event happened to arrive first.
	script.Invariant(
		"the cell's own local TopoServer is never deleted",
		func(ev ctrltest.Event) error {
			if ev.Type == "deleted" && ev.Kind == "TopoServer" &&
				ev.Key.Name == localTopoName {
				return errLocalTopoDeleted
			}
			return nil
		},
	)

	ctrltest.KnownDefect(t,
		"pkg/cluster-handler/controller/multigrescluster/reconcile_global.go:113 "+
			"(external-global-topo prune selector has no component filter or "+
			"owner-reference check, so it also deletes a cell's own local TopoServer)",
		func() error {
			stepErr := script.TryStep(
				"the cell controller creates its own local TopoServer and the "+
					"toposerver controller settles its status on it",
				func() error {
					return externalGlobalTopoCluster(t, ns, clusterName)
				},
				// The toposerver controller's whole settling sequence on a
				// TopoServer it has just been handed, measured over four runs
				// against a cluster whose global topology is managed so this
				// prune never fires, which is the one way to observe what the
				// object does when it is left alone: the first condition, then
				// the client and peer endpoints once the etcd StatefulSet
				// exists, then Ready once DataPlaneSim has ticked that
				// StatefulSet ready. Three writes, in that order, then quiet
				// indefinitely.
				//
				// The paths are the narrowest that pick out one write each.
				// status.conditions[0] belongs only to the first and
				// status.clientService only to the second; the third's paths
				// are a subset of the second's, so it is matched by
				// elimination, which is what assignEvents does a search rather
				// than a greedy first match for.
				//
				// No ordering is declared between them even though one was
				// observed, because ordering is opt-in for changes that follow
				// from the code and nothing here needs it: the deletion is
				// caught by the invariant above, not by an order violation.
				ctrltest.Added("TopoServer", localTopoName),
				ctrltest.Changed("TopoServer", localTopoName, "status.conditions[0].type"),
				ctrltest.Changed("TopoServer", localTopoName,
					"status.clientService", "status.peerService"),
				ctrltest.Changed("TopoServer", localTopoName, "status.phase"),
			)
			// TryFinish runs whatever the step returned, so that the script
			// ends with Finish exactly once and the end-of-script backstop is
			// satisfied on every path through this body. While the defect is
			// live the step returns long before the object has finished
			// settling, so the end of the script still has to be closed.
			//
			// The horizon is not load-bearing in either direction, which is the
			// point of choosing it freely. While the defect is live nothing
			// depends on it: the delete lands about 15ms after the create,
			// inside the step. Once the defect is fixed this is the only window
			// left in which a later prune pass could still be caught, and a
			// longer horizon can only refuse more events, never permit one.
			finishErr := script.TryFinish(10 * time.Second)

			switch {
			case errors.Is(stepErr, errLocalTopoDeleted):
				return stepErr
			case errors.Is(finishErr, errLocalTopoDeleted):
				return finishErr
			case stepErr != nil:
				// Anything else is this test's own declaration or pacing
				// rather than evidence about the operator, and a pin that
				// confirmed on it would survive the fix it is supposed to
				// expire on. Fatalf is the right side of the line
				// Script.fatalf already draws for the same reason.
				t.Fatalf("the script's declaration of the toposerver "+
					"controller's settling sequence did not hold, which is a "+
					"fact about this test rather than about the prune it pins: %v",
					stepErr)
			case finishErr != nil:
				t.Fatalf("the script's end was not quiet, and not because of "+
					"the deletion this test pins, which is a fact about this "+
					"test rather than about the prune: %v", finishErr)
			}
			return nil
		},
	)
}

// TestSelectorImpostorShardOwnerRefReconcileAdoptsUnrelatedPVC pins a new
// defect found by this sweep: reconcilePVCOwnerRefs
// (shard_controller.go:589) lists PersistentVolumeClaims by the shard's four
// identity labels alone (cluster, database, tablegroup, shard; no pool, no
// component), so it cannot tell the shard's own PVC from any other object
// carrying the same identity, and the only test it applies to a match before
// adopting it is whether that match already carries a ref with this shard's
// UID. A PVC belonging to nobody fails that test and is adopted, whenever the
// shard's effective PVCDeletionPolicy resolves to Delete.
//
// The bound on the defect is worth stating precisely, because it is what a fix
// has to be aimed at. There is an ownership check in this path:
// ctrl.SetControllerReference returns AlreadyOwnedError when the object already
// carries a different controller ownerRef, so this code does not take a PVC
// that belongs to someone else. What it takes is a PVC with no controller owner
// at all. The missing check is not "does this belong to somebody else" but
// "does this belong to me", and the selector is what cannot answer it.
//
// The impostor is a bare PersistentVolumeClaim carrying just those four
// labels and no pool label, the same shape shard_controller.go's own
// "shared backup PVC" branch expects, and no owner reference at all: the
// shape a PVC left behind by some other process, or a since-recreated
// resource under the same identity, would plausibly have.
//
// RequireQuiescent runs before the script's watch opens so the baseline it
// replays is the shard's own already-settled PVCs (its pool data PVCs and
// its backup PVC), named explicitly rather than guessed: this suite's own
// discipline is that an already-populated namespace's replay is the first
// step's problem to permit, not something to dodge by racing the watch
// ahead of convergence. Unlike the TopoServer test above, that is available
// here, because the shard does converge.
//
// The impostor itself is created after the watch opens, and its own
// creation-then-bind is declared with Before: DataPlaneSim
// (pkg/ctrltest/datasim.go) binds every PersistentVolumeClaim in the cluster
// regardless of who it belongs to, as a stand-in for the volume provisioner
// envtest does not run, and that status patch (status.phase/accessModes/
// capacity) has to be permitted explicitly or it is indistinguishable from
// the actual ownerRef adoption this test is pinning. Declaring the pair
// with Before, rather than waiting for Bound out-of-band first, is what
// keeps the watch open across the one window where the real defect could
// otherwise race in unobserved, immediately after creation and before this
// suite's own fake gets to it.
func TestSelectorImpostorShardOwnerRefReconcileAdoptsUnrelatedPVC(t *testing.T) {
	ns := Suite.Namespace(t)
	MinimalCluster(t, ns, "pvc-adopt")

	var shard multigresv1alpha1.Shard
	ctrltest.Eventually(t, 30*time.Second, "the cluster's one Shard to exist", func() error {
		shards := &multigresv1alpha1.ShardList{}
		if err := Suite.Client.List(
			t.Context(), shards, client.InNamespace(ns),
		); err != nil {
			return err
		}
		if len(shards.Items) != 1 {
			return fmt.Errorf("want exactly one Shard, got %d", len(shards.Items))
		}
		shard = shards.Items[0]
		return nil
	})

	Suite.RequireQuiescent(t, ns, time.Second, 30*time.Second)

	existingPVCs := &corev1.PersistentVolumeClaimList{}
	if err := Suite.Client.List(
		t.Context(), existingPVCs, client.InNamespace(ns),
	); err != nil {
		t.Fatalf("list existing PVCs: %v", err)
	}

	impostor := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "impostor-shared-pvc",
			Namespace: ns,
			Labels: map[string]string{
				metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
				metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
				metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
				metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	allow := make([]ctrltest.Allow, 0, len(existingPVCs.Items)+1)
	for _, pvc := range existingPVCs.Items {
		allow = append(allow, ctrltest.Added("PersistentVolumeClaim", pvc.Name))
	}
	allow = append(allow, ctrltest.Before(
		ctrltest.Added("PersistentVolumeClaim", impostor.Name),
		// Narrowed to status.phase rather than left open: DataPlaneSim's bind
		// always touches it alongside accessModes/capacity, so naming it is
		// enough to identify that write and that write only. Left open, this
		// leaf would also happily absorb the ownerRef adoption this test
		// exists to catch, since Changed with no paths matches any
		// modification at all.
		ctrltest.Changed("PersistentVolumeClaim", impostor.Name, "status.phase"),
	))

	script := Suite.NewScript(t, ns, &corev1.PersistentVolumeClaimList{})

	ctrltest.KnownDefect(t,
		"pkg/resource-handler/controller/shard/shard_controller.go:589 "+
			"(reconcilePVCOwnerRefs selects on the shard's four identity labels "+
			"alone, so it adopts any PVC carrying them that has no controller "+
			"ownerRef, with nothing establishing the PVC is the shard's own)",
		func() error {
			stepErr := script.TryStep(
				"baseline PVCs replay; the impostor is created, then bound like "+
					"any other PVC by this suite's data-plane fake",
				func() error {
					if err := Suite.Client.Create(t.Context(), impostor); err != nil {
						return err
					}
					// The impostor's own creation cannot trigger the shard's
					// reconcile loop (it carries no owner reference, so
					// Owns(&PersistentVolumeClaim{}) has nothing to map it back
					// to), and RequireQuiescent above means nothing else is left
					// to either: measured empirically, a fully quiesced shard
					// does not reconcile again on its own. A metadata-only nudge
					// on the Shard itself is what actually gets
					// reconcilePVCOwnerRefs to run again and look at the
					// impostor; this write is on ShardList, not the
					// PersistentVolumeClaimList this script watches, so it needs
					// no entry of its own in allow.
					shardCopy := shard.DeepCopy()
					patch := client.MergeFrom(shardCopy.DeepCopy())
					if shardCopy.Annotations == nil {
						shardCopy.Annotations = map[string]string{}
					}
					shardCopy.Annotations[selectorImpostorNudgeAnnotation] = time.Now().
						UTC().
						Format(time.RFC3339Nano)
					return Suite.Client.Patch(t.Context(), shardCopy, patch)
				},
				allow...,
			)
			// Run unconditionally, and 10s, for the reasons given at the same
			// call in the TopoServer test above. Unlike that test this one
			// needs no sentinel to tell two permitted-set outcomes apart: the
			// adoption is a modification of the impostor, and the only other
			// modification anything makes to it is DataPlaneSim's bind, which
			// the step permits by name and by path. So there is no second
			// route to a non-nil error through a permitted-set mismatch.
			//
			// That is narrower than "no second route at all", and the
			// difference matters for how much this pin can be trusted. A
			// TryStep timeout is also a non-nil error, and KnownDefect reads
			// any non-nil error as the defect still being live, so if the data
			// plane fake never binds the impostor or a baseline PVC name
			// drifts, this pin survives the operator fix that should have
			// retired it. That is the general limitation of pinned steps
			// stated on TryStep, and this call site is not exempt from it. The
			// TopoServer test's sentinel-plus-Fatalf discrimination is the
			// honest pattern if this ever needs to be tightened.
			finishErr := script.TryFinish(10 * time.Second)
			if stepErr != nil {
				return stepErr
			}
			return finishErr
		},
	)
}
