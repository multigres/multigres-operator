package suite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	multigresclustercontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
	tablegroupcontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/features"
	"github.com/multigres/multigres-operator/pkg/resolver"
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

// TestReadyForDeletionProtocol_OrphanCellDrains asserts that a Cell a settled
// cluster's spec does not name completes its drain handshake: multigrescluster
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
// when it is orphaned, and the Cell controller does not watch the
// MultigresCluster, so neither the Owns() watches nor the gate lift below
// reach it. A predicate that drops the annotation leaves the Cell annotated
// and never ready, and this fails at the deletion wait.
//
// No write a user can make produces an orphan Cell: the API server keeps
// spec.cells append-only and refuses to drop spec (TestCellsAppendOnly), so a
// Cell missing from spec exists only out of band, such as one created directly
// with the cluster's labels and owner. createOrphanCell makes exactly that,
// for secondSimCell, while the cluster is gated, so the Cell controller
// settles it while every multigrescluster pass is refused. Lifting the gate is
// the edit that has multigrescluster notice the orphan.
//
// The return trip appends secondSimCell to spec.cells after the drain and
// requires a fresh Cell, under the same name, that carries no PendingDeletion
// annotation and brings the cluster back to Healthy. envtest runs no garbage
// collector, so the deleted Cell's Deployment and Service outlive it here; the
// new Cell has to adopt or replace them, which a real cluster would have
// collected first.
func TestReadyForDeletionProtocol_OrphanCellDrains(t *testing.T) {
	c := newCase(t)
	cluster := c.newCluster("scenario-del-cell", pinPoolToDefaultCell)
	c.WaitForClusterHealthy(cluster)
	clusterKey := client.ObjectKeyFromObject(cluster)

	orphanCfg := CellConfig{Name: secondSimCell, ZoneID: "us-central1-b"}
	orphan := c.createOrphanCell(cluster, orphanCfg)
	orphanKey := client.ObjectKeyFromObject(orphan)
	orphanUID := orphan.UID

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
	c.setReconcileGate(&MultigresCluster{}, clusterKey, false)

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

	// Return trip: putting the cell into spec must yield a working Cell, not
	// one that inherits the drain.
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Cells = append(got.Spec.Cells, orphanCfg)
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
// into spec after its drain ran, but before multigrescluster deleted it,
// returns to normal service: PendingDeletion is cleared, the ReadyForDeletion
// the drain set is dropped, the managed local TopoServer the drain deleted is
// recreated, and the cleared condition is persisted before the TopoServer
// comes back, so no stored True ever describes a Cell that is no longer
// drained.
//
// The orphan is created out of band by createOrphanCell, for the reasons
// TestReadyForDeletionProtocol_OrphanCellDrains gives, and the restore is the
// append of its cell to spec.cells, which is the only way a cell can return to
// spec.
//
// The interleaving is forced with the reconcile gate rather than raced for.
// Gating the Cell lets multigrescluster stamp it without the Cell draining;
// gating the cluster then lets the Cell drain without the cluster deleting
// it, and keeps the append to spec.cells unseen until the gate lifts. The
// gate is a production mechanism, and lifting it is what an operator does,
// so every step runs on the real watch and reconcile paths.
//
// The Cell manages a local TopoServer so the drain has something to destroy:
// without one, handlePendingDeletion has nothing to delete and a stranded
// Cell would still look whole.
func TestReadyForDeletionProtocol_CellRestoredMidDrain(t *testing.T) {
	c := newCase(t)
	cluster := c.newCluster("scenario-del-cell-restore", pinPoolToDefaultCell)
	c.WaitForClusterHealthy(cluster)
	clusterKey := client.ObjectKeyFromObject(cluster)

	orphanCfg := CellConfig{
		Name:   secondSimCell,
		ZoneID: "us-central1-b",
		Spec: &multigresv1alpha1.CellInlineSpec{
			LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Replicas: ptr.To(int32(1))},
			},
		},
	}
	orphan := c.createOrphanCell(cluster, orphanCfg)
	cellKey := client.ObjectKeyFromObject(orphan)
	cellUID := orphan.UID
	localTopoKey := client.ObjectKey{
		Namespace: c.NS,
		Name:      cellcontroller.BuildLocalTopoServerName(orphan),
	}
	c.NoError(c.Get(localTopoKey, &multigresv1alpha1.TopoServer{}), "get local TopoServer")
	c.RequireQuiescent(2*time.Second, 30*time.Second)

	c.setReconcileGate(&multigresv1alpha1.Cell{}, cellKey, true)
	c.setReconcileGate(&MultigresCluster{}, clusterKey, false)
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
		got.Spec.Cells = append(got.Spec.Cells, orphanCfg)
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

// pinPoolToDefaultCell gives the cluster the default database, table group and
// shard explicitly, with the pool's cells pinned to defaultSimCell. A pool
// with no cells spans every cell in spec, so without the pin appending
// secondSimCell would grow the pool into it, and the suite's data plane does
// not bring that pool to full strength, leaving the cluster Progressing for
// reasons unrelated to the Cell under test.
func pinPoolToDefaultCell(spec *MultigresClusterSpec) {
	spec.Databases = []DatabaseConfig{{
		Name:    resolver.DefaultSystemDatabaseName,
		Default: true,
		TableGroups: []TableGroupConfig{{
			Name:    resolver.DefaultSystemTableGroupName,
			Default: true,
			Shards: []ShardConfig{{
				Name: "0-inf",
				Spec: &ShardInlineSpec{
					Pools: map[PoolName]PoolSpec{
						resolver.DefaultPoolName: {
							Type:  "readWrite",
							Cells: []CellName{defaultSimCell},
						},
					},
				},
			}},
		}},
	}}
}

// createOrphanCell creates a Cell for cfg, which the cluster's spec must not
// name, waits for the Cell controller to bring it to Healthy, and returns it.
//
// This is the out-of-band route to an orphan Cell, and the only one the API
// server leaves: spec.cells is append-only and spec cannot be removed, so no
// write to the MultigresCluster drops a cell. The Cell is the one
// reconcileCells would apply if cfg were appended to spec: the same resolver
// defaults the cluster and resolves cfg, BuildCell builds it with the cluster
// as owner, and the multigateway image and global topo ref, which
// multigrescluster resolves in memory and never writes back to the spec, are
// copied from the cluster's existing Cell. Creating it goes through the suite client, which
// the suite's lack of admission webhooks lets through.
//
// The cluster is gated before the Cell exists and is left gated on return,
// because an ungated multigrescluster stamps PendingDeletion on the Cell as
// soon as its cache holds it, before the Cell controller has created anything
// for a drain to tear down. The caller lifts the gate to have the orphan
// noticed.
func (c *C) createOrphanCell(cluster *MultigresCluster, cfg CellConfig) *multigresv1alpha1.Cell {
	c.Helper()
	clusterKey := client.ObjectKeyFromObject(cluster)
	existing := c.onlyCell(cluster)

	c.setReconcileGate(&MultigresCluster{}, clusterKey, true)
	c.waitForGateCached(&MultigresCluster{}, clusterKey)

	settled := &MultigresCluster{}
	c.NoError(c.Get(clusterKey, settled), "get gated cluster")
	allCellNames := []CellName{}
	for _, named := range settled.Spec.Cells {
		c.True(named.Name != cfg.Name,
			"spec.cells already names %s, so its Cell would not be an orphan", cfg.Name)
		allCellNames = append(allCellNames, named.Name)
	}
	allCellNames = append(allCellNames, cfg.Name)

	res := resolver.NewResolver(c.Client(), c.NS)
	_, err := res.PopulateClusterDefaults(c.Context(), settled)
	c.NoError(err, "populate cluster defaults")
	settled.Spec.Images.Multigateway = existing.Spec.Images.Multigateway
	cfg.CellTemplate = settled.Spec.EffectiveCellTemplate(cfg.CellTemplate)
	gateway, placement, localTopo, err := res.ResolveCell(c.Context(), settled, &cfg)
	c.NoError(err, "resolve cell %s", cfg.Name)

	cell, err := multigresclustercontroller.BuildCell(
		settled,
		&cfg,
		gateway,
		placement,
		localTopo,
		existing.Spec.GlobalTopoServer,
		allCellNames,
		Suite.Scheme,
	)
	c.NoError(err, "build orphan Cell %s", cfg.Name)
	c.NoError(c.Create(cell), "create orphan Cell %s", cell.Name)

	cellKey := client.ObjectKeyFromObject(cell)
	c.Eventually(30*time.Second, "orphan Cell "+cell.Name+" to report Healthy", func() error {
		if err := c.Get(cellKey, cell); err != nil {
			return err
		}
		if cell.Status.Phase != multigresv1alpha1.PhaseHealthy {
			return fmt.Errorf("phase is %q", cell.Status.Phase)
		}
		return nil
	})
	c.NoError(c.cellUntouched(cellKey, cell.UID),
		"orphan Cell %s before the gated cluster was released", cell.Name)
	return cell
}

// waitForGateCached blocks until the operator's cache holds the reconcile gate
// on key and every pass that read key before then has finished. It is the
// barrier for gating a settled object, where waitForGatedPass cannot work:
// the gate's event filter drops the label's own update event, and a settled
// object schedules no requeue, so no gated pass ever runs. Every controller
// reads through the one manager cache, so once it holds the label each later
// pass sees it and is refused. A pass that read the object earlier may still
// be running, and the quiet window waits that out: the multigrescluster
// controller's real passes always write, so one still in flight would break
// the quiet.
//
// Testkit gap: ctrltest files a pass only when it ends, so there is no view of
// passes still in flight. With one, this would wait for the passes on key that
// started before the label was cached to end, instead of for a quiet window.
func (c *C) waitForGateCached(obj client.Object, key client.ObjectKey) {
	c.Helper()
	cache := Suite.Manager("multigres-operator").Mgr.GetCache()
	c.Eventually(30*time.Second, "the operator's cache to hold the reconcile gate on "+key.String(),
		func() error {
			if err := cache.Get(c.Context(), key, obj); err != nil {
				return err
			}
			if obj.GetLabels()[features.ReconcileLabel] != features.Disabled {
				return fmt.Errorf("cached %s is not gated yet", key)
			}
			return nil
		})
	c.NoError(c.TryQuiescent(2*time.Second, 30*time.Second),
		"passes that read %s before the gate was cached to finish", key)
}
