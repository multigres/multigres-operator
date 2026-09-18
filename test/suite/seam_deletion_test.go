package suite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	multigresclustercontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
	tablegroupcontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/ctrltest"
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
	ns := Suite.Namespace(t)
	cluster := MinimalCluster(t, ns, "seam-del")

	ctrltest.Eventually(t, 30*time.Second, "cluster to report Healthy", func() error {
		got := &multigresv1alpha1.MultigresCluster{}
		key := client.ObjectKeyFromObject(cluster)
		if err := Suite.Client.Get(t.Context(), key, got); err != nil {
			return err
		}
		if got.Status.Phase != multigresv1alpha1.PhaseHealthy {
			return fmt.Errorf("phase is %q", got.Status.Phase)
		}
		return nil
	})

	// Copy the real TableGroup's resolved GlobalTopoServer ref and component
	// Images rather than re-deriving them: both are resolved in-memory once
	// per MultigresCluster reconcile (globalTopoRef by the unexported
	// globalTopoRef method, Images by resolveImages) and never written back to
	// MultigresCluster.Spec, so the cluster object this test already holds
	// still has them blank. Re-deriving either by hand risks building an
	// orphan that fails validation or that the topology store does not
	// recognize, for reasons unrelated to what this test asserts.
	realTGs := &multigresv1alpha1.TableGroupList{}
	if err := Suite.Client.List(t.Context(), realTGs, client.InNamespace(ns),
		client.MatchingLabels{"multigres.com/cluster": cluster.Name}); err != nil {
		t.Fatalf("list tablegroups: %v", err)
	}
	if len(realTGs.Items) != 1 {
		t.Fatalf(
			"want exactly one TableGroup before introducing an orphan, got %d",
			len(realTGs.Items),
		)
	}
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
	// run concurrently (seam_fanout_test.go documents the same discipline for
	// cursors, for the same reason), and multigrescluster can notice and
	// annotate an orphan TableGroup within the reconcile pass that follows its
	// creation. A stream opened even one line later could start listening
	// after that annotation, and the condition flips it drives, have already
	// landed, which is exactly the 4ms-window flake this replaces.
	st := Suite.Watch(t, ns, &multigresv1alpha1.TableGroupList{}, &multigresv1alpha1.ShardList{})

	orphanTG, err := multigresclustercontroller.BuildTableGroup(
		cluster,
		multigresv1alpha1.DatabaseConfig{Name: "postgres"},
		&multigresv1alpha1.TableGroupConfig{Name: "orphan"},
		[]multigresv1alpha1.ShardResolvedSpec{{
			Name: "0-inf",
			Multiorch: multigresv1alpha1.MultiorchSpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(0))},
				Cells:         []multigresv1alpha1.CellName{defaultSimCell},
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
		}},
		globalTopoRef,
		Suite.Scheme,
	)
	if err != nil {
		t.Fatalf("build orphan tablegroup: %v", err)
	}
	if err := Suite.Client.Create(t.Context(), orphanTG); err != nil {
		t.Fatalf("create orphan tablegroup: %v", err)
	}
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
	if err != nil {
		t.Fatalf("build orphan shard: %v", err)
	}
	if err := Suite.Client.Create(
		t.Context(),
		shardCR,
	); err != nil &&
		!apierrors.IsAlreadyExists(err) {
		t.Fatalf("create orphan shard: %v", err)
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
		if err != nil {
			t.Fatalf(
				"waiting for the protocol to reach Shard ready, then TableGroup ready, "+
					"then TableGroup deleted, in that order: %v",
				err,
			)
		}
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
				if !sawShardReady {
					t.Fatalf(
						"TableGroup %s reported ReadyForDeletion before Shard %s ever did",
						orphanKey.Name,
						orphanShardKey.Name,
					)
				}
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
	if err := st.Terminal(); err != nil {
		t.Fatalf("the event stream failed, so every assertion over it is void: %v", err)
	}

	if !sawShardReady {
		t.Fatalf(
			"TableGroup %s was deleted before Shard %s ever reported ReadyForDeletion",
			orphanKey.Name,
			orphanShardKey.Name,
		)
	}
	if !sawTableGroupReady {
		t.Fatalf(
			"TableGroup %s was deleted before it ever reported ReadyForDeletion",
			orphanKey.Name,
		)
	}

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
	if !deletedByCluster {
		t.Fatalf(
			"TableGroup %s disappeared without a recorded delete from multigrescluster",
			orphanKey.Name,
		)
	}
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
