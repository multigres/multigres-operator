package suite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	multigresclustercontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
	tablegroupcontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/features"
	cellcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
	"github.com/multigres/testkit/ctrltest"
)

// TestReadyForDeletionProtocol asserts the three-hop ReadyForDeletion protocol
// found in pkg/resource-handler/controller/shard/reconcile_deletion.go,
// pkg/cluster-handler/controller/tablegroup/tablegroup_controller.go and
// pkg/cluster-handler/controller/multigrescluster/reconcile_databases.go: a
// Shard sets multigresv1alpha1.ConditionReadyForDeletion on itself once every
// pool pod has drained, its parent TableGroup sets the same condition on
// itself once every child Shard has, and MultigresCluster deletes a TableGroup
// only once that TableGroup reports it.
//
// This is orphan pruning, not whole-cluster teardown. Deleting a
// MultigresCluster goes through MultigresClusterReconciler.handleDeletion,
// which lists and Deletes its Cells and TableGroups directly and never
// consults ConditionReadyForDeletion at all; that path was added in
// da7d639177b0 as a narrower fix scoped explicitly to "orphan pruning" (its
// own commit message), for the case where a TableGroup or Cell falls out of a
// still-live cluster's spec and must drain before it is safe to remove. A
// whole-cluster delete has nothing left to protect by draining, so it tears
// down directly instead. The three-hop protocol is therefore reachable only
// through that narrower path, which this test drives directly: it builds an
// orphan TableGroup (one MultigresCluster.Spec.Databases entry will never
// name) with multigresclustercontroller.BuildTableGroup, the same builder
// production code uses, so multigrescluster's reconcileDatabases treats it
// exactly as it would treat a TableGroup a user just removed from spec.
func TestReadyForDeletionProtocol(t *testing.T) {
	c := newCase(t)
	ns := c.NS
	cluster := c.MinimalCluster("scenario-del")

	c.WaitForClusterHealthy(cluster)

	// Copy the real TableGroup's resolved GlobalTopoServer ref and component
	// Images rather than re-deriving them: both are resolved in-memory once
	// per MultigresCluster reconcile (globalTopoRef by the unexported
	// globalTopoRef method, Images by resolveImages) and never written back to
	// MultigresCluster.Spec, so the cluster object this test already holds
	// still has them blank. Re-deriving either by hand risks building an
	// orphan that fails validation or that the topology store does not
	// recognize, for reasons unrelated to what this test asserts.
	realTGs := &TableGroupList{}
	c.NoError(c.List(realTGs,
		client.MatchingLabels{"multigres.com/cluster": cluster.Name}),
		"list tablegroups")
	c.Len(realTGs.Items, 1,
		"want exactly one TableGroup before introducing an orphan")
	globalTopoRef := realTGs.Items[0].Spec.GlobalTopoServer
	cluster.Spec.Images = multigresv1alpha1.ClusterImages{
		Multiorch:        realTGs.Items[0].Spec.Images.Multiorch,
		Multipooler:      realTGs.Items[0].Spec.Images.Multipooler,
		Postgres:         realTGs.Items[0].Spec.Images.Postgres,
		ImagePullPolicy:  realTGs.Items[0].Spec.Images.ImagePullPolicy,
		ImagePullSecrets: realTGs.Items[0].Spec.Images.ImagePullSecrets,
	}

	// The orphan's one shard has no pools and zero Multiorch replicas, so it
	// creates no Pods. That keeps the pod drain state machine, which is its
	// own protocol, out of this test's way: with zero pods,
	// ShardReconciler.handlePendingDeletion takes the "no pods" branch and
	// sets ConditionReadyForDeletion on its very first pass. What this test
	// asserts is the condition handoff between the three controllers, not
	// how long draining a pod takes.
	//
	// Multiorch.Cells is set explicitly because getMultiorchCells
	// (shard_controller.go) falls back to the union of pool cells when it is
	// empty, and errors out when that is empty too; with no pools, leaving
	// Cells unset turns every normal (non-deletion) reconcile of this Shard
	// into a reconcile error, which is retried on controller-runtime's own
	// backoff rather than this suite's compressed one and made the protocol's
	// timing depend on that backoff instead of on the protocol.
	//
	// Opened before the orphan TableGroup exists, not after: five reconcilers
	// run concurrently (scenario_fanout_test.go documents the same discipline for
	// cursors, for the same reason), and multigrescluster can notice and
	// annotate an orphan TableGroup within the reconcile pass that follows its
	// creation. A stream opened even one line later could start listening
	// after that annotation, and the condition flips it drives, have already
	// landed, which is exactly the 4ms-window flake this replaces.
	st := c.Watch(&TableGroupList{}, &ShardList{})

	orphanTG, err := multigresclustercontroller.BuildTableGroup(
		cluster,
		DatabaseConfig{Name: "postgres"},
		&TableGroupConfig{Name: "orphan"},
		[]multigresv1alpha1.ShardResolvedSpec{{
			Name: "0-inf",
			Multiorch: multigresv1alpha1.MultiorchSpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(0))},
				Cells:         []CellName{defaultSimCell},
			},
			Pools: map[PoolName]PoolSpec{},
		}},
		globalTopoRef,
		Suite.Scheme,
	)
	c.NoError(err, "build orphan tablegroup")
	c.NoError(c.Create(orphanTG), "create orphan tablegroup")
	orphanKey := client.ObjectKeyFromObject(orphanTG)

	// Pre-create the child Shard, from the now-persisted orphanTG (so its
	// owner reference carries a real UID), immediately after the TableGroup
	// rather than leaving TableGroupReconciler to create it on its first
	// normal pass. Without this, a real race exists: if multigrescluster
	// annotates the brand-new TableGroup with AnnotationPendingDeletion before
	// TableGroupReconciler has ever run stepApplyDesiredShards for it,
	// handlePendingDeletion lists zero child Shards and reports
	// ReadyForDeletion vacuously, having consulted nothing. Creating the Shard
	// here, keyed identically to what stepApplyDesiredShards would build,
	// guarantees the child the protocol is supposed to drain exists before
	// either controller's watch can fire. AlreadyExists is fine rather than
	// fatal: it means TableGroupReconciler's own first pass won the race and
	// applied this same Shard first, which is the other safe ordering.
	shardCR, err := tablegroupcontroller.BuildShard(
		orphanTG,
		&orphanTG.Spec.Shards[0],
		Suite.Scheme,
	)
	c.NoError(err, "build orphan shard")
	if err := c.Create(shardCR); err != nil &&
		!apierrors.IsAlreadyExists(err) {
		c.NoError(err, "create orphan shard")
	}
	orphanShardKey := client.ObjectKeyFromObject(shardCR)
	clusterKey := client.ObjectKeyFromObject(cluster)

	// Corroborating evidence, independent of how fast the three controllers
	// converge: the interceptor records every reconcile pass, including ones
	// that wrote nothing, so a pass that asked to be woken again in 5s while
	// something was still pending is permanent history even if the object it
	// was about is deleted moments later. Both waits are scoped by object key,
	// so neither can be satisfied by the pre-existing healthy
	// TableGroup/Shard's own unrelated reconciles.
	//
	// What this actually proves is narrower than it looks: both
	// TableGroupReconciler.handlePendingDeletion and reconcileDatabases also
	// take a 5s-requeue path the first time they see an orphan (setting the
	// PendingDeletion annotation itself sets `allReady`/`pendingDeletion` and
	// requeues), so mutating away only the later
	// `meta.IsStatusConditionTrue(...ConditionReadyForDeletion)` guard still
	// leaves that earlier requeue in place and these two waits keep passing -
	// verified by making exactly that mutation in each function and watching
	// these two lines stay green while the poll below caught it instead. What
	// these two waits do rule out is a version that deletes an orphan in the
	// very same pass that first notices it, with no intervening wait at all.
	Suite.Reconciles.WaitForRequeue(t, "tablegroup", orphanKey, 5*time.Second, 10*time.Second)
	Suite.Reconciles.WaitForRequeue(
		t,
		"multigrescluster",
		clusterKey,
		5*time.Second,
		10*time.Second,
	)

	// Primary evidence for the specific guard on each hop, read off the event
	// stream rather than sampled by polling: the write-up measured the
	// TableGroup's parent deleting it 4.3ms after ReadyForDeletion is set,
	// against a 20ms poll, and showed no poll interval fixes that because one
	// sample already costs about as long as the state persists. The stream is
	// push rather than sample, so it sees the transition however briefly it
	// held. sawX latches record having observed each condition true at least
	// once, so that reaching a later state (the TableGroup gone) without ever
	// having latched an earlier one (its own condition, or its child Shard's)
	// is still caught even if every step landed inside one 50ms reorder
	// window, or before this loop's first read.
	//
	// Mutation verified: deleting the
	// `if !meta.IsStatusConditionTrue(s.Status.Conditions,
	// multigresv1alpha1.ConditionReadyForDeletion) { allReady = false }` guard
	// in TableGroupReconciler.handlePendingDeletion, or the equivalent guard
	// over item.Status.Conditions in reconcileDatabases, each independently
	// makes this fail (tried one at a time): the TableGroup reports
	// ReadyForDeletion, or is deleted, before its Shard's own condition is
	// ever observed true.
	sawShardReady := false
	sawTableGroupReady := false
	tgGone := false
	deadline := time.Now().Add(30 * time.Second)
	for !tgGone {
		ev, err := st.Next(time.Until(deadline))
		c.NoError(err,
			"waiting for the protocol to reach Shard ready, then TableGroup ready, "+
				"then TableGroup deleted, in that order")
		switch {
		case ev.Key == orphanShardKey && ev.Kind == "Shard":
			if conditionSetTrue(ev, string(multigresv1alpha1.ConditionReadyForDeletion)) {
				sawShardReady = true
			}
		case ev.Key == orphanKey && ev.Kind == "TableGroup":
			if ev.Type == "deleted" {
				tgGone = true
				continue
			}
			if conditionSetTrue(ev, string(multigresv1alpha1.ConditionReadyForDeletion)) {
				c.True(sawShardReady,
					"TableGroup %s reported ReadyForDeletion before Shard %s ever did",
					orphanKey.Name, orphanShardKey.Name)
				sawTableGroupReady = true
			}
		}
	}
	// A relist can win the race against the last blocked read: Next selects
	// over the event channel and the failure channel, and Go picks at random
	// when both are ready. If it hands back the deletion, the loop exits and
	// the entry guard never runs again, so the terminal error would go
	// unobserved and the assertions below would blame the operator for history
	// the harness lost.
	c.NoError(st.Terminal(),
		"the event stream failed, so every assertion over it is void")

	c.True(sawShardReady,
		"TableGroup %s was deleted before Shard %s ever reported ReadyForDeletion",
		orphanKey.Name, orphanShardKey.Name)
	c.True(sawTableGroupReady,
		"TableGroup %s was deleted before it ever reported ReadyForDeletion",
		orphanKey.Name)

	// Attribution check: confirm the delete that made the TableGroup
	// disappear was actually issued by multigrescluster, using a static scan
	// of the completed op log rather than a live cursor wait. A live
	// CursorFor(ns, "multigrescluster") wait was not usable for any step
	// above: a single MultigresCluster reconcile pass unconditionally
	// re-applies the healthy default Cell and TableGroup before ever reaching
	// the orphan-pruning loop, so the next op in that scope is legitimately
	// something else almost every time, and WaitForNext does not skip ahead
	// to find a match (that is WaitForMatching, which this suite marks as an
	// escape hatch not to be reached for). The op log has already stopped
	// growing with respect to this object by the time we reach this check, so
	// a static scan carries none of Cursor's live-ordering caveats.
	deletedByCluster := false
	for _, op := range Suite.Ops.OpsInNamespace(ns) {
		if op.Controller == "multigrescluster" && op.Verb == "delete" &&
			ctrltest.KindSuffix(op.Kind) == "TableGroup" && op.Key.Name == orphanKey.Name {
			deletedByCluster = true
			break
		}
	}
	c.True(deletedByCluster,
		"TableGroup %s disappeared without a recorded delete from multigrescluster",
		orphanKey.Name)
}

// TestReadyForDeletionProtocol_OrphanCellDrains asserts that a Cell which falls
// out of a settled cluster's spec completes its drain handshake: multigrescluster
// stamps AnnotationPendingDeletion on it, the Cell controller's own reconcile
// answers with ReadyForDeletion, and multigrescluster then deletes it.
//
// The stamp is a merge patch that changes only that annotation: no generation
// bump, no project-ref change, no label change, no status wipe. So the Cell
// controller's For predicate is the only thing that can turn it into a
// reconcile, and the test makes sure nothing else does. The Cell's last pass
// before the edit must have asked for no requeue; this harness clamps every
// RequeueAfter to RequeueClamp, so a Cell that was still rescheduling itself
// would rerun within milliseconds and drain regardless of the predicate.
// Nothing about the Cell's owned TopoServer, Deployment or Service changes
// when it is orphaned, so the Owns() watches stay silent too. A predicate
// that drops the annotation leaves the Cell annotated and never ready, and
// this fails at the deletion wait.
//
// The orphan is produced through the API server the way a user can produce
// it: the append-only rule on MultigresClusterSpec.Cells
// (oldSelf.all(c, c.name in self.map(x, x.name))) is a transition rule on an
// optional field, so the API server skips it when the new object has no
// spec.cells at all, and a JSON patch removing the field is accepted. If that
// gap is ever closed, this test fails at the patch below rather than passing
// vacuously, and needs another route to an orphan Cell.
//
// The return trip restores spec.cells after the drain and requires a fresh
// Cell, under the same name, that carries no PendingDeletion annotation and
// brings the cluster back to Healthy. envtest runs no garbage collector, so
// the deleted Cell's Deployment and Service outlive it here; the new Cell has
// to adopt or replace them, which a real cluster would have collected first.
func TestReadyForDeletionProtocol_OrphanCellDrains(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("scenario-del-cell")
	c.WaitForClusterHealthy(cluster)

	settled := &MultigresCluster{}
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), settled), "get settled cluster")
	originalCells := settled.Spec.Cells
	c.Len(originalCells, 1, "want exactly one cell in spec before orphaning it")

	cells := &multigresv1alpha1.CellList{}
	c.NoError(
		c.List(cells, client.MatchingLabels{"multigres.com/cluster": cluster.Name}),
		"list cells",
	)
	c.Len(cells.Items, 1, "want exactly one Cell before orphaning it")
	orphanKey := client.ObjectKeyFromObject(&cells.Items[0])
	orphanUID := cells.Items[0].UID

	// Quiescence lets every write-triggered reconcile finish; it cannot see
	// a nil-error requeue that writes nothing, so the Cell's own last pass is
	// checked for one separately.
	c.RequireQuiescent(2*time.Second, 30*time.Second)
	var lastPass *ctrltest.Reconcile
	for _, r := range Suite.Reconciles.InNamespace(c.NS) {
		if r.Controller == "cell" && !r.Gated && r.Request.NamespacedName == orphanKey {
			lastPass = &r
		}
	}
	c.NotNil(lastPass, "no recorded reconcile of Cell %s", orphanKey.Name)
	c.True(lastPass.Err == nil && lastPass.RequestedAfter == 0,
		"Cell %s's last pass before the edit asked to run again (err=%v, after=%s), "+
			"which could drain it without the predicate", orphanKey.Name,
		lastPass.Err, lastPass.RequestedAfter)

	editAt := time.Now()
	removeCells := []byte(`[{"op":"remove","path":"/spec/cells"}]`)
	c.NoError(
		c.Patch(settled, client.RawPatch(types.JSONPatchType, removeCells)),
		"remove spec.cells",
	)

	c.Eventually(30*time.Second, "orphaned Cell "+orphanKey.Name+" to drain and be deleted",
		func() error {
			cell := &multigresv1alpha1.Cell{}
			err := c.Get(orphanKey, cell)
			if apierrors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			return fmt.Errorf("still present: pendingDeletion=%q readyForDeletion=%v",
				cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion],
				meta.IsStatusConditionTrue(cell.Status.Conditions,
					multigresv1alpha1.ConditionReadyForDeletion))
		})

	// The deletion above is only correct if it went through the handshake:
	// the Cell controller wrote its own status after the edit, and
	// multigrescluster, which gates its delete on ReadyForDeletion, issued
	// the delete. Polled rather than scanned once, because the recorder logs
	// a write when the call returns, which can be after the API server has
	// already made the Cell's absence visible to the read above.
	c.Eventually(5*time.Second, "the drain of Cell "+orphanKey.Name+" to be attributed",
		func() error {
			cellAnswered, clusterDeleted := false, false
			for _, op := range Suite.Ops.OpsInNamespace(c.NS) {
				if op.Key != orphanKey || ctrltest.KindSuffix(op.Kind) != "Cell" ||
					op.At.Before(editAt) {
					continue
				}
				switch {
				case op.Controller == "cell" && strings.HasPrefix(op.Verb, "status-"):
					cellAnswered = true
				case op.Controller == "multigrescluster" && op.Verb == "delete":
					clusterDeleted = true
				}
			}
			if !cellAnswered {
				return fmt.Errorf("no status write from the cell controller after the edit")
			}
			if !clusterDeleted {
				return fmt.Errorf("no delete recorded from multigrescluster")
			}
			return nil
		})

	// Return trip: putting the cell back must yield a working Cell, not one
	// that inherits the drain.
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Cells = originalCells
	})
	c.WaitForClusterHealthy(cluster)
	c.Eventually(30*time.Second, "Cell "+orphanKey.Name+" to be recreated and Healthy",
		func() error {
			cell := &multigresv1alpha1.Cell{}
			if err := c.Get(orphanKey, cell); err != nil {
				return err
			}
			if cell.UID == orphanUID {
				return fmt.Errorf("still the orphaned Cell (uid %s)", orphanUID)
			}
			if v := cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion]; v != "" {
				return fmt.Errorf("recreated Cell carries PendingDeletion=%q", v)
			}
			if cell.Status.Phase != multigresv1alpha1.PhaseHealthy {
				return fmt.Errorf("phase is %q", cell.Status.Phase)
			}
			return nil
		})
	c.RequireQuiescent(2*time.Second, 30*time.Second)
}

// conditionSetTrue reports whether ev records a status.conditions entry whose
// type field arrived at conditionType, with that same entry's status field
// arrived at "True", in the same event.
//
// Changed and Transitions carry a diff, not the object's current state, so
// the type and the status of one array slot have to be read out of the same
// event to know which condition moved; a status flip alone does not say
// which condition it belongs to. Requiring the type to change in the same
// event rather than looking it up separately is sound here because every
// condition this test watches for (Shard and TableGroup's own
// ConditionReadyForDeletion) is only ever set once, straight to True: neither
// controller ever writes it False first, so its type always appears fresh
// alongside the status that makes it true.
//
// If that premise ever breaks, and a controller sets the condition False
// before True, this helper stops recognising the transition and the test fails
// with an ordering complaint against the operator rather than against itself.
// So a sudden "reported ReadyForDeletion before X ever did" failure is worth
// checking here second: the Cell controller already writes a condition False
// first, so the premise holds by habit rather than by rule.
func conditionSetTrue(ev ctrltest.Event, conditionType string) bool {
	wantType := `"` + conditionType + `"`
	for path, transition := range ev.Transitions {
		// The status.conditions prefix is checked as well as the leaf, so a
		// future array of objects carrying both a type and a status field
		// cannot start feeding this helper silently. No such array exists on
		// either status today; the guard is what keeps the doc comment above
		// true rather than merely true for now.
		if !strings.HasPrefix(path, "status.conditions[") ||
			!strings.HasSuffix(path, "].type") || transition.To != wantType {
			continue
		}
		statusPath := strings.TrimSuffix(path, "type") + "status"
		if status, ok := ev.Transitions[statusPath]; ok && status.To == `"True"` {
			return true
		}
	}
	return false
}

// TestReadyForDeletionProtocol_CellRestoredMidDrain asserts that a Cell put
// back into spec after its drain ran, but before multigrescluster deleted it,
// returns to normal service: PendingDeletion is cleared, the ReadyForDeletion
// the drain set is dropped, the managed local TopoServer the drain deleted is
// recreated, and the cleared condition is persisted before the TopoServer
// comes back, so no stored True ever describes a Cell that is no longer
// drained.
//
// The interleaving is forced with the reconcile gate rather than raced for.
// Gating the Cell lets multigrescluster stamp it without the Cell draining;
// gating the cluster then lets the Cell drain without the cluster deleting
// it, and keeps the restore of spec.cells unseen until the gate lifts. The
// gate is a production mechanism, and lifting it is what an operator does,
// so every step runs on the real watch and reconcile paths.
//
// The Cell manages a local TopoServer so the drain has something to destroy:
// without one, handlePendingDeletion has nothing to delete and a stranded
// Cell would still look whole.
func TestReadyForDeletionProtocol_CellRestoredMidDrain(t *testing.T) {
	c := newCase(t)
	cluster := c.newCluster("scenario-del-cell-restore", func(spec *MultigresClusterSpec) {
		spec.Cells[0].Spec = &multigresv1alpha1.CellInlineSpec{
			LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Replicas: ptr.To(int32(1))},
			},
		}
	})
	c.WaitForClusterHealthy(cluster)

	settled := &MultigresCluster{}
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), settled), "get settled cluster")
	originalCells := settled.Spec.Cells

	cells := &multigresv1alpha1.CellList{}
	c.NoError(
		c.List(cells, client.MatchingLabels{"multigres.com/cluster": cluster.Name}),
		"list cells",
	)
	c.Len(cells.Items, 1, "want exactly one Cell")
	cellKey := client.ObjectKeyFromObject(&cells.Items[0])
	cellUID := cells.Items[0].UID
	localTopoKey := client.ObjectKey{
		Namespace: c.NS,
		Name:      cellcontroller.BuildLocalTopoServerName(&cells.Items[0]),
	}
	c.NoError(c.Get(localTopoKey, &multigresv1alpha1.TopoServer{}), "get local TopoServer")
	c.RequireQuiescent(2*time.Second, 30*time.Second)

	c.setReconcileGate(&multigresv1alpha1.Cell{}, cellKey, true)
	c.NoError(
		c.Patch(settled, client.RawPatch(types.JSONPatchType,
			[]byte(`[{"op":"remove","path":"/spec/cells"}]`))),
		"remove spec.cells",
	)
	c.Eventually(30*time.Second, "multigrescluster to stamp PendingDeletion", func() error {
		cell := &multigresv1alpha1.Cell{}
		if err := c.Get(cellKey, cell); err != nil {
			return err
		}
		if cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
			return fmt.Errorf("not stamped yet")
		}
		return nil
	})

	clusterKey := client.ObjectKeyFromObject(cluster)
	c.setReconcileGate(&MultigresCluster{}, clusterKey, true)
	c.waitForGatedPass("multigrescluster", clusterKey)
	c.setReconcileGate(&multigresv1alpha1.Cell{}, cellKey, false)
	c.Eventually(30*time.Second, "Cell to drain its local TopoServer and report ready",
		func() error {
			cell := &multigresv1alpha1.Cell{}
			if err := c.Get(cellKey, cell); err != nil {
				return err
			}
			if !meta.IsStatusConditionTrue(cell.Status.Conditions,
				multigresv1alpha1.ConditionReadyForDeletion) {
				return fmt.Errorf("ReadyForDeletion not True")
			}
			err := c.Get(localTopoKey, &multigresv1alpha1.TopoServer{})
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("local TopoServer still present (err=%v)", err)
			}
			return nil
		})

	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Cells = originalCells
	})
	restoredAt := time.Now()
	c.setReconcileGate(&MultigresCluster{}, clusterKey, false)

	c.Eventually(60*time.Second, "restored Cell to return to normal service", func() error {
		cell := &multigresv1alpha1.Cell{}
		if err := c.Get(cellKey, cell); err != nil {
			return err
		}
		if cell.UID != cellUID {
			return fmt.Errorf("Cell was replaced (uid %s, want %s)", cell.UID, cellUID)
		}
		if v := cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion]; v != "" {
			return fmt.Errorf("PendingDeletion still set: %q", v)
		}
		if meta.FindStatusCondition(cell.Status.Conditions,
			multigresv1alpha1.ConditionReadyForDeletion) != nil {
			return fmt.Errorf("ReadyForDeletion condition still present")
		}
		if err := c.Get(localTopoKey, &multigresv1alpha1.TopoServer{}); err != nil {
			return fmt.Errorf("local TopoServer: %w", err)
		}
		if cell.Status.Phase != multigresv1alpha1.PhaseHealthy {
			return fmt.Errorf("phase is %q", cell.Status.Phase)
		}
		return nil
	})
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(2*time.Second, 30*time.Second)

	// The Cell controller must persist the cleared condition before it
	// recreates the local TopoServer. Otherwise a stored True could outlive
	// the drain it describes: a status write that failed, or a removal that
	// landed mid-pass, would leave ReadyForDeletion True on a Cell whose
	// TopoServer is back, and multigrescluster would delete it undrained.
	clearIdx, recreateIdx := -1, -1
	for i, op := range Suite.Ops.OpsInNamespace(c.NS) {
		if op.Controller != "cell" || op.Err != nil || op.At.Before(restoredAt) {
			continue
		}
		switch {
		case clearIdx < 0 && op.Key == cellKey && ctrltest.KindSuffix(op.Kind) == "Cell" &&
			strings.HasPrefix(op.Verb, "status-"):
			clearIdx = i
		case recreateIdx < 0 && op.Key == localTopoKey &&
			ctrltest.KindSuffix(op.Kind) == "TopoServer" && op.Verb != "delete":
			recreateIdx = i
		}
	}
	c.True(recreateIdx >= 0, "no recorded write recreating local TopoServer %s", localTopoKey.Name)
	c.True(clearIdx >= 0 && clearIdx < recreateIdx,
		"Cell %s recreated its local TopoServer (op %d) before persisting the cleared "+
			"ReadyForDeletion (op %d)", cellKey.Name, recreateIdx, clearIdx)
}

// setReconcileGate sets or clears multigres.com/reconcile=disabled on one
// object, pausing or resuming every controller that reconciles it. obj is
// only a typed receiver for the read.
func (c *C) setReconcileGate(obj client.Object, key client.ObjectKey, disabled bool) {
	c.Helper()
	c.NoError(c.Get(key, obj), "get "+key.String()+" to gate it")
	patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	if disabled {
		labels[features.ReconcileLabel] = features.Disabled
	} else {
		delete(labels, features.ReconcileLabel)
	}
	obj.SetLabels(labels)
	c.NoError(c.Patch(obj, patch), "set reconcile gate on "+key.String())
}

// waitForGatedPass blocks until controller has run a pass against key that
// started after this call and wrote nothing, which is what a pass refused by
// the reconcile gate looks like. The gate label reaches a controller through
// its cache, so a pass already running, or one that read the cache before the
// label arrived, can still act. Each controller here runs one pass at a time,
// so once a gated pass has ended every earlier pass has too, and every later
// one reads a cache that already holds the label. It is meant for a
// controller whose real passes always write; the multigrescluster controller
// applies its Cells and TableGroups on every one.
//
// Testkit gap: ctrltest has no barrier for "controller X's cache has observed
// object Y at or after resourceVersion N". With one, this would wait on the
// label patch's resourceVersion directly instead of inferring it from a
// write-free pass, and would work for controllers whose real passes can be
// write-free too.
func (c *C) waitForGatedPass(controller string, key client.ObjectKey) {
	c.Helper()
	since := time.Now()
	c.Eventually(30*time.Second, controller+" to observe the reconcile gate on "+key.String(),
		func() error {
			for _, r := range Suite.Reconciles.InNamespace(c.NS) {
				if r.Controller == controller && r.Request.NamespacedName == key &&
					r.Start.After(since) && !r.End.IsZero() && r.Err == nil &&
					len(Suite.Reconciles.Ops(r)) == 0 {
					return nil
				}
			}
			return fmt.Errorf("no write-free pass since %s", since.Format(time.RFC3339Nano))
		})
}
