package multigrescluster

import (
	"errors"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/multiadmin"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func commerceIn(state string, id int64) []multiadmin.TableGroup {
	return []multiadmin.TableGroup{
		{ID: id, Name: "commerce", Shards: []string{"0-80", "80-inf"}, State: state},
	}
}

// listSequence returns lists[i] on the i-th read and the last one after.
func listSequence(lists ...[]multiadmin.TableGroup) func(string) ([]multiadmin.TableGroup, error) {
	i := 0
	return func(string) ([]multiadmin.TableGroup, error) {
		l := lists[min(i, len(lists)-1)]
		i++
		return l, nil
	}
}

func (f *dynamicFixture) claim(id int64, provisioner string) {
	f.t.Helper()
	if err := f.topo.ClaimTableGroupProvisioning(f.t.Context(), "postgres",
		&clustermetadatapb.TableGroupProvisionClaim{
			Provisioner: provisioner, TablegroupId: id, TablegroupName: "commerce",
		}); err != nil {
		f.t.Fatal(err)
	}
}

func (f *dynamicFixture) claimed(id int64) *clustermetadatapb.TableGroupProvisionClaim {
	f.t.Helper()
	c, err := f.topo.GetTableGroupProvisionClaim(f.t.Context(), "postgres", id)
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		return nil
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return c
}

// dynamicShard is a Shard of a dynamic tablegroup as the TableGroup
// controller builds it, left behind while its finalizer runs.
func dynamicShard(id string) *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{ObjectMeta: metav1.ObjectMeta{
		Name: "test-cluster-postgres-commerce-0-80", Namespace: "default",
		Labels: map[string]string{
			metadata.LabelMultigresCluster:           "test-cluster",
			metadata.LabelMultigresDatabase:          "postgres",
			metadata.LabelMultigresTableGroup:        "commerce",
			multigresv1alpha1.LabelDynamicTableGroup: "true",
		},
		Annotations: map[string]string{multigresv1alpha1.AnnotationTableGroupID: id},
	}}
}

func TestDynamicTableGroups_ClaimsBeforeProvisioning(t *testing.T) {
	f := newDynamicFixture(t, true, desiredCommerce(7))
	f.reconcile()

	if f.tableGroup("commerce") == nil {
		t.Fatal("commerce not provisioned")
	}
	c := f.claimed(7)
	if c == nil {
		t.Fatal("provisioned without a claim")
	}
	if c.GetProvisioner() != provisionerID(f.cluster) || c.GetTablegroupName() != "commerce" {
		t.Errorf("claim = %v", c)
	}
	if f.calls != 2 {
		t.Errorf("multiadmin read %d times, want the list and a re-read after claiming", f.calls)
	}

	// Converged: nothing pending, no further claim or re-read.
	f.calls = 0
	f.reconcile()
	if f.calls != 1 {
		t.Errorf("multiadmin read %d times on a converged pass, want once", f.calls)
	}
}

func TestDynamicTableGroups_RecheckNotProvisionableReleasesTheClaim(t *testing.T) {
	for name, second := range map[string][]multiadmin.TableGroup{
		"dropping": commerceIn(multiadmin.StateDropping, 7),
		"gone":     nil,
	} {
		t.Run(name, func(t *testing.T) {
			f := newDynamicFixture(t, true,
				listSequence(commerceIn(multiadmin.StateCreating, 7), second))
			f.reconcile()

			if f.tableGroup("commerce") != nil {
				t.Error("provisioned although the re-read no longer lists it as creating")
			}
			if f.claimed(7) != nil {
				t.Error("claim left behind")
			}
		})
	}
}

func TestDynamicTableGroups_FailedRecheckKeepsTheClaimUntilDropped(t *testing.T) {
	reads := 0
	f := newDynamicFixture(t, true, func(string) ([]multiadmin.TableGroup, error) {
		reads++
		switch reads {
		case 1:
			return commerceIn(multiadmin.StateCreating, 7), nil
		case 2:
			return nil, errors.New("HTTP 503")
		}
		return commerceIn(multiadmin.StateDropping, 7), nil
	})
	f.reconcile()

	if f.tableGroup("commerce") != nil {
		t.Error("provisioned without a successful re-read")
	}
	if f.claimed(7) == nil {
		t.Error("claim released on a failed read: the catalog's state is unknown")
	}
	c := meta.FindStatusCondition(f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups)
	if c == nil || c.Reason != "DesiredStateUnknown" {
		t.Errorf("condition = %+v, want DesiredStateUnknown", c)
	}

	// The next read lists it dropping and nothing was provisioned.
	f.reconcile()
	if f.claimed(7) != nil {
		t.Error("claim of a dropped id with nothing provisioned not released")
	}
}

func TestDynamicTableGroups_ClaimHeldElsewhereProvisionsNothing(t *testing.T) {
	f := newDynamicFixture(t, true, desiredCommerce(7))
	f.claim(7, "someone-else")
	f.reconcile()

	if f.tableGroup("commerce") != nil {
		t.Error("provisioned an id another provisioner holds")
	}
	if f.claimed(7).GetProvisioner() != "someone-else" {
		t.Error("another provisioner's claim was replaced")
	}
	if !meta.IsStatusConditionFalse(f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups) {
		t.Error("condition does not report the failed claim")
	}
}

func TestDynamicTableGroups_DroppingTearsDownThenReleases(t *testing.T) {
	f := newDynamicFixture(t, true, listSequence(commerceIn(multiadmin.StateDropping, 7)),
		existingDynamic("test-cluster", "commerce", "7"))
	f.claim(7, provisionerID(f.cluster))

	f.reconcile()
	if !pendingDeletion(f.tableGroup("commerce")) {
		t.Fatal("a tablegroup the catalog is dropping was not marked for deletion")
	}
	if f.claimed(7) == nil {
		t.Fatal("claim released while the TableGroup exists")
	}

	// The TableGroup is gone; one Shard is still running its finalizer.
	if err := f.c.Delete(t.Context(), f.tableGroup("commerce")); err != nil {
		t.Fatal(err)
	}
	shard := dynamicShard("7")
	if err := f.c.Create(t.Context(), shard); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if f.claimed(7) == nil {
		t.Fatal("claim released while a Shard carrying the id exists")
	}

	if err := f.c.Delete(t.Context(), shard); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if f.claimed(7) != nil {
		t.Error("claim not released once nothing carries the id")
	}
	// Idempotent: the catalog may list it a while longer.
	f.reconcile()
}

func TestDynamicTableGroups_DroppingLeavesAnotherProvisionersClaim(t *testing.T) {
	f := newDynamicFixture(t, true, listSequence(commerceIn(multiadmin.StateDropping, 7)))
	f.claim(7, "someone-else")
	f.reconcile()
	if f.claimed(7) == nil {
		t.Error("released a claim another provisioner holds")
	}
	p, _ := newPoller(f)
	if p.clusterNeedsReconcile(t.Context(), f.cluster) {
		t.Error("another provisioner's claim asked for a reconcile")
	}
}

func TestDynamicTableGroups_DroppingAnotherIDLeavesTheTableGroup(t *testing.T) {
	f := newDynamicFixture(t, true, listSequence(commerceIn(multiadmin.StateDropping, 9)),
		existingDynamic("test-cluster", "commerce", "7"), dynamicShard("7"))
	f.claim(7, provisionerID(f.cluster))
	f.claim(9, provisionerID(f.cluster))
	f.reconcile()

	if pendingDeletion(f.tableGroup("commerce")) {
		t.Error("dropping id 9 marked the TableGroup of id 7")
	}
	if f.claimed(7) == nil {
		t.Error("claim of id 7 released")
	}
	if f.claimed(9) != nil {
		t.Error("claim of id 9, with nothing carrying it, not released")
	}
}

func TestDynamicTableGroups_UnknownStateIsNeitherProvisionedNorRemoved(t *testing.T) {
	f := newDynamicFixture(t, true, listSequence([]multiadmin.TableGroup{
		{
			ID:     7,
			Name:   "commerce",
			Shards: []string{"0-inf"},
			State:  "TABLE_GROUP_STATE_UNSPECIFIED",
		},
		{ID: 8, Name: "orders", Shards: []string{"0-inf"}},
	}), existingDynamic("test-cluster", "commerce", "7"))
	f.reconcile()

	if pendingDeletion(f.tableGroup("commerce")) {
		t.Error("a tablegroup in an unknown state was removed")
	}
	if f.tableGroup("orders") != nil {
		t.Error("a tablegroup with no state was provisioned")
	}
}

func TestDynamicTableGroups_GateOffDropsNothing(t *testing.T) {
	f := newDynamicFixture(t, false, listSequence(commerceIn(multiadmin.StateDropping, 7)),
		existingDynamic("test-cluster", "commerce", "7"))
	f.reconcile()
	if pendingDeletion(f.tableGroup("commerce")) {
		t.Error("removed a tablegroup with the opt-in off")
	}
}

func TestPoller_DroppedTableGroups(t *testing.T) {
	f := newDynamicFixture(t, true, listSequence(commerceIn(multiadmin.StateDropping, 7)),
		existingDynamic("test-cluster", "commerce", "7"))
	f.claim(7, provisionerID(f.cluster))
	p, _ := newPoller(f)
	ctx := t.Context()

	if !p.clusterNeedsReconcile(ctx, f.cluster) {
		t.Fatal("a dropped tablegroup still serving did not ask for a reconcile")
	}
	f.reconcile()
	if p.clusterNeedsReconcile(ctx, f.cluster) {
		t.Fatal("a draining tablegroup asked for a reconcile; its own events drive it")
	}

	if err := f.c.Delete(ctx, f.tableGroup("commerce")); err != nil {
		t.Fatal(err)
	}
	if !p.clusterNeedsReconcile(ctx, f.cluster) {
		t.Fatal("a held claim with no TableGroup left did not ask for a reconcile")
	}
	f.reconcile()
	if f.claimed(7) != nil {
		t.Fatal("claim not released")
	}
	if p.clusterNeedsReconcile(ctx, f.cluster) {
		t.Fatal("a released drop asked for a reconcile")
	}
}
