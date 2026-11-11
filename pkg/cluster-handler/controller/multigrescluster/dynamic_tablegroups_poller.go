package multigrescluster

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// dynamicTableGroupPollInterval is how often the poller reads every opted-in
// cluster's desired lists. It also bounds one cluster's reads as a whole.
const dynamicTableGroupPollInterval = 10 * time.Second

// maxConcurrentPolls bounds how many clusters are read at once per tick.
const maxConcurrentPolls = 8

// dynamicTableGroupPoller is the manager runnable that replaces requeueing
// opted-in clusters. Every interval it reads each opted-in cluster's desired
// tablegroups from multiadmin and enqueues the cluster, through the channel
// source wired in SetupWithManagerReconciler, only when the catalog asks for
// something the cluster's TableGroups and conditions do not yet show. The
// reconcile stays event-driven: a converged cluster is never reconciled, and
// so never re-applied, because of the catalog.
//
// The reads leave the reconcile path entirely. One deadline covers all of a
// cluster's databases and clusters are read concurrently, so a multiadmin
// that drops packets costs one interval per tick, not a worker for ten
// seconds per database.
type dynamicTableGroupPoller struct {
	r        *MultigresClusterReconciler
	events   chan<- event.GenericEvent
	interval time.Duration
}

// NeedLeaderElection keeps the poller on the leader, with the controllers it
// feeds.
func (p *dynamicTableGroupPoller) NeedLeaderElection() bool { return true }

// Start runs the poller until ctx ends.
func (p *dynamicTableGroupPoller) Start(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

// poll reads every opted-in, ungated cluster once and enqueues the ones that
// need a reconcile.
func (p *dynamicTableGroupPoller) poll(ctx context.Context) {
	l := log.FromContext(ctx).WithName("dynamic-tablegroup-poller")
	clusters := &multigresv1alpha1.MultigresClusterList{}
	if err := p.r.List(ctx, clusters); err != nil {
		l.Error(err, "Failed to list clusters")
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrentPolls)
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		if !dynamicTableGroupsEnabled(cluster) || cluster.DeletionTimestamp != nil {
			continue
		}
		// A gated cluster is not this operator's to read, and the channel
		// source's event filter would drop the event anyway.
		if gated, err := p.r.Gate.Gated(ctx, p.r.Client, cluster); gated || err != nil {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if !p.clusterNeedsReconcile(ctx, cluster) {
				return
			}
			select {
			case p.events <- event.GenericEvent{Object: cluster}:
			case <-ctx.Done():
			}
		}()
	}
	wg.Wait()
}

// clusterNeedsReconcile reports whether the catalog asks something of cluster
// that its TableGroups and conditions do not yet show: an entry with no
// TableGroup of its name and id, a problem or conflict the conditions do not
// report yet, or a read that fails while the condition still says synced.
// Everything the conditions already report is left alone: a lasting problem
// does not reconcile the cluster every tick.
func (p *dynamicTableGroupPoller) clusterNeedsReconcile(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) bool {
	// One deadline for all of the cluster's databases.
	ctx, cancel := context.WithTimeout(ctx, p.interval)
	defer cancel()
	l := log.FromContext(ctx).WithName("dynamic-tablegroup-poller").
		WithValues("cluster", client.ObjectKeyFromObject(cluster))

	tgs := &multigresv1alpha1.TableGroupList{}
	if err := p.r.List(ctx, tgs,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"multigres.com/cluster": cluster.Name},
	); err != nil {
		l.Error(err, "Failed to list tablegroups")
		return false
	}
	existing := make(map[string]*multigresv1alpha1.TableGroup, len(tgs.Items))
	for i := range tgs.Items {
		existing[tgs.Items[i].Name] = &tgs.Items[i]
	}

	failureReported := meta.IsStatusConditionPresentAndEqual(cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups, metav1.ConditionFalse)
	conflictReported := meta.IsStatusConditionTrue(cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroupConflict)

	for _, db := range cluster.Spec.Databases {
		desired, err := p.r.listDesiredTableGroups(ctx, cluster, string(db.Name))
		if err != nil {
			if !failureReported {
				return true
			}
			continue
		}
		plan := planDesiredTableGroups(cluster, db, desired, existing)
		switch {
		case len(plan.pending) > 0,
			len(plan.problems) > 0 && !failureReported,
			len(plan.conflicts) > 0 && !conflictReported:
			return true
		}
	}
	return false
}
