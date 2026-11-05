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
//     TestReadyForDeletionProtocol in scenario_deletion_test.go.
//
//   - reconcile_topology.go:245    CellList        {cluster}
//     Read-only: collects names of cells pending deletion, for topology
//     pruning. Never mutates or deletes anything itself. SAFE.
//
//   - reconcile_global.go:120      TopoServerList  {cluster, component=global-topo}
//     FIXED. Was a confirmed live defect: no component filter and no
//     owner-reference check meant that, when global topology is external,
//     every TopoServer carrying the cluster label was deleted, including a
//     cell's own local TopoServer (component "local-topo"), which the old
//     selector could not tell apart from the managed global one (component
//     "global-topo"). The selector now also requires the global-topo
//     component label, and the delete now checks controller ownership by the
//     cluster, which a cell's local TopoServer never has (it is
//     controller-owned by its Cell). TESTED below:
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
//     This is SAFE while reconcilePVCOwnerRefs below needs a provenance
//     check on what looks like the same evidence, an unowned PVC being acted
//     on, and the two verdicts are worth reading together rather than one at
//     a time. The difference is documented intent, which is the only thing
//     that can separate them: :439 says an unowned PVC carrying these labels
//     is precisely what it exists to sweep, whereas reconcilePVCOwnerRefs'
//     own doc comment and its call site in Reconcile both say it exists to
//     fix up ownerRefs on the shard's own PVCs across a mid-lifecycle policy
//     change. Acting on a stranger is the job in one and an accident in the
//     other.
//
//   - multigrescluster_controller.go:603  MultigresClusterList, InNamespace
//     only. Not a label selector (a map function for CoreTemplate/
//     CellTemplate/ShardTemplate change fanout). Out of scope.
//
//   - certificate.go:394 (via pkg/util/certs.List)  no label selector,
//     InNamespace only. The eventual delete (certs.Prune) checks
//     OwnedBy(cert, ownerUID) before deleting anything, the same discipline
//     the TopoServer prune above now also applies. SAFE.
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
//   - shard_controller.go (reconcilePVCOwnerRefs)  PersistentVolumeClaimList
//     {cluster, database, tablegroup, shard} (no pool, no component). The
//     selector cannot tell the shard's own PVC from any other carrying the
//     same identity, and SetControllerReference only refuses a PVC that
//     already has a controller (AlreadyOwnedError), so a PVC with no
//     controller would be adopted, and garbage-collected with the Shard,
//     whenever the effective PVCDeletionPolicy resolves to Delete. FIXED by
//     a name check rather than a narrower selector: only PVCs carrying a name
//     the shard's builders produce (isShardBuiltPVC, pool_pvc.go) get or keep
//     the ownerRef. A component filter would still adopt a stray carrying
//     the component label too. TESTED below:
//     TestSelectorImpostorShardOwnerRefReconcileLeavesStrangerPVCs.
//
//   - reconcile_deletion.go:57   DeploymentList  {cluster, database,
//     tablegroup, shard}, in the Shard's own handleDeletion. Raw delete, no
//     owner-ref check. Same family as reconcilePVCOwnerRefs. Not
//     independently tested given this pass's budget.
//
//   - reconcile_deletion.go:90   PodList  same 4-key selector, same
//     function. Raw delete, no owner-ref check. Lower interference risk
//     than the multigrescluster Cell/TableGroup case above, since a Shard's
//     own teardown is not cascaded through another controller's graceful
//     orphan protocol. Not tested here.
//
//   - reconcile_deletion.go (cleanupShardPVCs)  PersistentVolumeClaimList
//     same 4-key selector. Matches are marked orphan, or deleted while the
//     cluster itself is being deleted. Same family as reconcilePVCOwnerRefs
//     at a different lifecycle point, and FIXED by the same name check,
//     which shardPVCShouldBeCleaned applies for both. Unit tested
//     (TestHandleDeletion, reconcile_deletion_test.go); not tested here.
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
//     reconcilePVCOwnerRefs.
//
//   - reconcile_pool_pods.go:50   PodList  pool+cell-scoped
//     (buildPoolLabelsWithCell). DEFECT of the same class as
//     reconcilePVCOwnerRefs, and the strongest untested candidate left in
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
//     has not been derived (tickPVCs's is, for
//     TestSelectorImpostorShardOwnerRefReconcileLeavesStrangerPVCs below). Naming it SAFE, as an earlier revision of this block
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
	"maps"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	topopkg "github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
	"github.com/multigres/testkit/ctrltest"
)

// errLocalTopoDeleted is what the TopoServer script's invariant returns when
// it sees the deletion this test asserts against.
//
// A sentinel rather than a match on the harness's message text, so the
// failure this test reports names the exact violation (the cell's own local
// TopoServer being deleted) rather than whichever error the script's
// declaration or pacing happened to produce.
var errLocalTopoDeleted = errors.New(
	"the cell's own local TopoServer was deleted",
)

// externalGlobalTopoCluster creates a MultigresCluster whose global topology is
// external and whose one cell manages its own local TopoServer: what makes
// reconcileGlobalTopoServer's desired-is-nil branch run on every reconcile,
// while still giving the cell controller a local TopoServer of its own to
// keep reapplying.
//
// It returns an error rather than calling t.Fatalf as MinimalCluster does,
// because its caller runs it inside a Script step's do: a Fatalf there would
// Goexit out of the middle of a script, whereas Script.TryStep routes a
// failing do to the test's own Fatalf.
//
// The password Secret is created here rather than shared with MinimalCluster
// because the two fixtures differ in every other field; what is worth keeping
// in step is the deliberate choice to leave it unlabelled, which is what a user
// would create and what makes the reconcilers' APIReader necessary.
func (c *C) externalGlobalTopoCluster(clusterName string) error {
	c.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: adminSecretName, Namespace: c.NS},
		StringData: map[string]string{"password": "postgres"},
	}
	if err := c.Create(secret); err != nil {
		return fmt.Errorf("create password secret: %w", err)
	}

	cluster := &MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: c.NS},
		Spec: MultigresClusterSpec{
			PostgresPasswordSecretRef: PostgresPasswordSecretRef{
				Name: adminSecretName,
				Key:  "password",
			},
			PVCDeletionPolicy: &PVCDeletionPolicy{
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
			Cells: []CellConfig{
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
	if err := c.Create(cluster); err != nil {
		return fmt.Errorf("create MultigresCluster: %w", err)
	}
	return nil
}

// TestSelectorImpostorGlobalTopoPruneDeletesCellOwnedLocalTopoServer asserts
// that the external-global-topo prune in reconcile_global.go never deletes a
// cell's own local TopoServer. It used to: the prune listed TopoServers by
// cluster label alone (no component filter, no owner-reference check) and
// deleted every match whenever global topology is external, and a cell's own
// local TopoServer carries that same cluster label. The selector now also
// requires the global-topo component label, and the delete now requires
// controller ownership by the cluster, which a cell's local TopoServer never
// has.
//
// The impostor here is not synthetic: it is the real local TopoServer the cell
// controller legitimately creates and keeps reapplying, which is exactly what
// made the fixed defect "a permanent create/delete loop" rather than a
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
// leaves a deletion as the one event nothing accounts for, and leaves nothing
// to wait out.
func TestSelectorImpostorGlobalTopoPruneDeletesCellOwnedLocalTopoServer(t *testing.T) {
	c := newCase(t)
	const clusterName = "ext-global-topo"
	cellResourceName := name.JoinWithConstraints(
		name.DefaultConstraints, clusterName, string(defaultSimCell),
	)
	localTopoName := topopkg.ManagedLocalTopoServerName(cellResourceName)

	script := c.NewScript(&TopoServerList{})

	// The deletion is this test's entire claim, so it is asserted directly
	// rather than inferred from being whatever event no step happened to
	// permit.
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

	stepErr := script.TryStep(
		"the cell controller creates its own local TopoServer and the "+
			"toposerver controller settles its status on it",
		func() error {
			return c.externalGlobalTopoCluster(clusterName)
		},
		// The toposerver controller's whole settling sequence on a TopoServer
		// it has just been handed: the first condition, then the client and
		// peer endpoints once the etcd StatefulSet exists, then the
		// StatefulSet's pods existing but none Ready yet (DataPlaneSim holds
		// a new pod Pending for one sweep, as kubelet would), then Ready once
		// they are. Four writes, in that order, then quiet indefinitely.
		//
		// The paths are the narrowest that pick out one write each.
		// status.conditions[0] belongs only to the first and
		// status.clientService only to the second; the third's paths are a
		// subset of the second's, so it is matched by elimination, which is
		// why assignEvents does a search rather than a greedy first match.
		//
		// No ordering is declared between them even though one was observed,
		// because ordering is opt-in for changes that follow from the code
		// and nothing here needs it: a deletion is caught by the invariant
		// above, not by an order violation.
		ctrltest.Added("TopoServer", localTopoName),
		ctrltest.Changed("TopoServer", localTopoName, "status.conditions[0].type"),
		ctrltest.Changed("TopoServer", localTopoName,
			"status.clientService", "status.peerService"),
		ctrltest.Changed("TopoServer", localTopoName,
			"status.conditions[1].message", "status.message"),
		ctrltest.Changed("TopoServer", localTopoName, "status.phase"),
	)
	c.NoError(stepErr, "the cell's local TopoServer did not settle as declared")

	// The prune runs on every reconcile while global topology is external, so
	// the window after the step is where a reintroduced defect would show. A
	// longer horizon can only refuse more events, never permit one.
	c.NoError(script.TryFinish(10*time.Second),
		"the script's end was not quiet, so something is still writing")
}

// TestSelectorImpostorShardOwnerRefReconcileLeavesStrangerPVCs drives
// reconcilePVCOwnerRefs (shard_controller.go) through a PVCDeletionPolicy
// round trip, Delete to Retain and back, with three stranger PVCs carrying
// the shard's four identity labels in the namespace, and asserts that the
// shard's own PVCs follow the policy each way while the strangers are never
// written to.
//
// The selector there is the shard's four identity labels alone, so it lists
// whatever carries them. What keeps a stranger out is the name check before
// adoption (isShardBuiltPVC, pool_pvc.go): adopting gives the PVC a
// controller ref to the Shard, and garbage collection then deletes it with
// the Shard. The strangers cover the shapes that matter:
//
//   - impostor-shared-pvc: the shared backup PVC's labels (no pool label)
//     under another name, and no owner at all.
//   - impostor-data-pvc: a pool data PVC's labels, pool and cell included,
//     under a name no pod index produces, and no owner at all.
//   - a-impostor-controlled: controlled by another owner. Named to list first,
//     because the loop stops at its first error and SetControllerReference
//     refuses an object with another controller: an attempt to adopt it would
//     wedge every PVC after it, the shard's own included, as well as fail.
//
// The script opens after RequireQuiescent, so its watch replays the shard's
// already-settled PVCs as Added events, which the first step permits by name.
// Each stranger's create is permitted together with DataPlaneSim's bind
// (status.accessModes, status.capacity and status.phase in one patch, as the
// volume provisioner envtest does not run would), and naming status.phase is
// enough to pick that patch out. The step cannot end until both have arrived
// for all three, so the bind never trails into a later step or into Finish.
//
// The two policy steps are what make the strangers' silence mean something:
// each one makes reconcilePVCOwnerRefs run with the strangers already in its
// list, as the ownerRef change on each of the shard's own PVCs proves, and
// the closed world fails on any write to a stranger in either direction.
func TestSelectorImpostorShardOwnerRefReconcileLeavesStrangerPVCs(t *testing.T) {
	c := newCase(t)
	ns := c.NS
	cluster := c.MinimalCluster("pvc-adopt")

	shard := c.awaitShard()

	c.RequireQuiescent(time.Second, 30*time.Second)

	existingPVCs := &corev1.PersistentVolumeClaimList{}
	c.NoError(c.List(existingPVCs), "list existing PVCs")

	// The shard's own PVCs are the ones it controls under MinimalCluster's
	// Delete policy. The global topology's etcd PVCs belong to its
	// StatefulSet and are not the shard's to touch.
	var own []string
	var dataTemplate *corev1.PersistentVolumeClaim
	sawBackup := false
	for i := range existingPVCs.Items {
		pvc := &existingPVCs.Items[i]
		if !metav1.IsControlledBy(pvc, &shard) {
			continue
		}
		own = append(own, pvc.Name)
		if pvc.Labels[metadata.LabelMultigresPool] == "" {
			sawBackup = true
		} else if dataTemplate == nil {
			dataTemplate = pvc
		}
	}
	c.True(sawBackup, "MinimalCluster's shard controls no shared backup PVC")
	c.NotNil(dataTemplate, "MinimalCluster's shard controls no pool data PVC")

	identity := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}
	stranger := func(pvcName string, labels map[string]string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: ns,
				Labels:    maps.Clone(labels),
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
	}
	sharedImpostor := stranger("impostor-shared-pvc", identity)
	dataImpostor := stranger("impostor-data-pvc", dataTemplate.Labels)
	// The owner has to exist: this suite collects garbage, and a PVC whose
	// owner is missing is deleted.
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "someone-else", Namespace: ns}}
	c.NoError(c.Create(owner), "create the controlled stranger's owner")
	controlled := stranger("a-impostor-controlled", identity)
	controlled.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "v1",
		Kind:       "ConfigMap",
		Name:       owner.Name,
		UID:        owner.UID,
		Controller: ptr.To(true),
	}}
	strangers := []*corev1.PersistentVolumeClaim{sharedImpostor, dataImpostor, controlled}

	script := c.NewScript(&corev1.PersistentVolumeClaimList{})
	script.StepTimeout = 10 * time.Second

	created := make([]ctrltest.Allow, 0, len(existingPVCs.Items)+len(strangers))
	for _, pvc := range existingPVCs.Items {
		created = append(created, ctrltest.Added("PersistentVolumeClaim", pvc.Name))
	}
	for _, pvc := range strangers {
		created = append(created, ctrltest.Before(
			ctrltest.Added("PersistentVolumeClaim", pvc.Name),
			ctrltest.Changed("PersistentVolumeClaim", pvc.Name, "status.phase"),
		))
	}
	script.Step(
		"baseline PVCs replay; the strangers are created, then bound like any "+
			"other PVC by this suite's data-plane fake",
		func() error {
			for _, pvc := range strangers {
				if err := c.Create(pvc); err != nil {
					return err
				}
			}
			return nil
		},
		created...,
	)

	ownerRefChanged := make([]ctrltest.Allow, 0, len(own))
	for _, pvcName := range own {
		ownerRefChanged = append(ownerRefChanged, ctrltest.Changed(
			"PersistentVolumeClaim", pvcName, "metadata.ownerReferences[0].uid",
		))
	}
	setPolicy := func(policy multigresv1alpha1.PVCRetentionPolicyType) func() error {
		return func() error {
			return retry.RetryOnConflict(retry.DefaultRetry, func() error {
				got := &MultigresCluster{}
				if err := c.Get(client.ObjectKeyFromObject(cluster), got); err != nil {
					return err
				}
				got.Spec.PVCDeletionPolicy = &PVCDeletionPolicy{
					WhenDeleted: policy,
					WhenScaled:  policy,
				}
				return c.Update(got)
			})
		}
	}
	script.Step(
		"Retain releases the shard's own PVCs and writes to no stranger",
		setPolicy(multigresv1alpha1.RetainPVCRetentionPolicy),
		ownerRefChanged...,
	)
	script.Step(
		"Delete adopts the shard's own PVCs again and writes to no stranger",
		setPolicy(multigresv1alpha1.DeletePVCRetentionPolicy),
		ownerRefChanged...,
	)
	// Long enough for the shard controller's requeues after the last policy
	// change, so an adoption a later pass made would land here.
	script.Finish(10 * time.Second)

	for _, pvcName := range own {
		got := &corev1.PersistentVolumeClaim{}
		c.NoError(c.Get(client.ObjectKey{Namespace: ns, Name: pvcName}, got), "get %s", pvcName)
		c.Check().True(metav1.IsControlledBy(got, &shard),
			"the shard's own PVC %s is not controlled by it after the round trip", pvcName)
	}
	for _, want := range strangers {
		got := &corev1.PersistentVolumeClaim{}
		c.NoError(c.Get(client.ObjectKeyFromObject(want), got), "get %s", want.Name)
		c.Check().EqDiff(want.OwnerReferences, got.OwnerReferences,
			"stranger PVC %s owner references changed", want.Name)
	}
}
