package multigrescluster

import (
	"context"
	"errors"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/multiadmin"
)

func newPoller(f *dynamicFixture) (*dynamicTableGroupPoller, chan event.GenericEvent) {
	events := make(chan event.GenericEvent, 8)
	return &dynamicTableGroupPoller{r: f.r, events: events, interval: dynamicTableGroupPollInterval}, events
}

func TestPoller_EnqueuesOnlyWhenTheCatalogAsksForSomethingNew(t *testing.T) {
	f := newDynamicFixture(t, true, desiredCommerce(7))
	p, _ := newPoller(f)

	if !p.clusterNeedsReconcile(t.Context(), f.cluster) {
		t.Fatal("a tablegroup the catalog lists and the cluster lacks did not ask for a reconcile")
	}

	f.reconcile()
	if p.clusterNeedsReconcile(t.Context(), f.cluster) {
		t.Fatal("a converged cluster asked for a reconcile")
	}

	// The same name under a new catalog id is a new incarnation.
	f.r.ListDesiredTableGroups = func(context.Context, *multigresv1alpha1.MultigresCluster, string) ([]multiadmin.TableGroup, error) {
		return desiredCommerce(9)("")
	}
	if !p.clusterNeedsReconcile(t.Context(), f.cluster) {
		t.Fatal("an id change did not ask for a reconcile")
	}
}

func TestPoller_ReportedProblemsDoNotRequeue(t *testing.T) {
	bad := func(string) ([]multiadmin.TableGroup, error) {
		return []multiadmin.TableGroup{{ID: 1, Name: "Has_Upper", Shards: []string{"0-inf"}}}, nil
	}
	f := newDynamicFixture(t, true, bad)
	p, _ := newPoller(f)

	if !p.clusterNeedsReconcile(t.Context(), f.cluster) {
		t.Fatal("an unreported problem did not ask for a reconcile")
	}
	f.reconcile() // reports it: condition False
	if p.clusterNeedsReconcile(t.Context(), f.cluster) {
		t.Fatal("a problem the condition already reports asked for a reconcile every tick")
	}

	// A failing read behaves the same: once reported, it is left alone.
	f.r.ListDesiredTableGroups = func(context.Context, *multigresv1alpha1.MultigresCluster, string) ([]multiadmin.TableGroup, error) {
		return nil, errors.New("HTTP 503")
	}
	if p.clusterNeedsReconcile(t.Context(), f.cluster) {
		t.Fatal("a read failure asked for a reconcile while the condition was already False")
	}
	f.cluster.Status.Conditions = nil
	if !p.clusterNeedsReconcile(t.Context(), f.cluster) {
		t.Fatal("a read failure with the condition absent did not ask for a reconcile")
	}
}

func TestPoller_SpecConflictRequeuesUntilReported(t *testing.T) {
	f := newDynamicFixture(t, true, desiredCommerce(7))
	f.reconcile()
	f.cluster.Spec.Databases[0].TableGroups = append(
		f.cluster.Spec.Databases[0].TableGroups,
		multigresv1alpha1.TableGroupConfig{
			Name:   "commerce",
			Shards: []multigresv1alpha1.ShardConfig{{Name: "0-inf"}},
		},
	)
	p, _ := newPoller(f)
	if !p.clusterNeedsReconcile(t.Context(), f.cluster) {
		t.Fatal("an unreported spec conflict did not ask for a reconcile")
	}
	f.reconcile()
	if p.clusterNeedsReconcile(t.Context(), f.cluster) {
		t.Fatal("a reported spec conflict asked for a reconcile every tick")
	}
}

func TestPoller_PollSkipsClustersNotOptedIn(t *testing.T) {
	f := newDynamicFixture(t, false, desiredCommerce(7))
	p, events := newPoller(f)
	p.poll(t.Context())
	if f.calls != 0 || len(events) != 0 {
		t.Fatalf("opt-in off: %d reads, %d events", f.calls, len(events))
	}

	f.cluster.Annotations = map[string]string{multigresv1alpha1.AnnotationDynamicTableGroups: "true"}
	if err := f.c.Update(t.Context(), f.cluster); err != nil {
		t.Fatal(err)
	}
	p.poll(t.Context())
	if len(events) != 1 {
		t.Fatalf("opt-in on with a new tablegroup: %d events, want 1", len(events))
	}
	e := <-events
	if client.ObjectKeyFromObject(e.Object) != client.ObjectKeyFromObject(f.cluster) {
		t.Errorf("enqueued %v, want %v", client.ObjectKeyFromObject(e.Object), client.ObjectKeyFromObject(f.cluster))
	}

	// A cluster being deleted is left alone. A finalizer keeps the object
	// around with its deletionTimestamp set.
	f.cluster.Finalizers = []string{"test"}
	if err := f.c.Update(t.Context(), f.cluster); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Delete(t.Context(), f.cluster); err != nil {
		t.Fatal(err)
	}
	calls := f.calls
	p.poll(t.Context())
	if f.calls != calls || len(events) != 0 {
		t.Fatalf("deleting cluster: %d new reads, %d events", f.calls-calls, len(events))
	}
}
