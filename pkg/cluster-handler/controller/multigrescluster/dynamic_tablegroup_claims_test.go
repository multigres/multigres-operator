package multigrescluster

import (
	"errors"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(f.t, f.topo.ClaimTableGroupProvisioning(f.t.Context(), "postgres",
		&clustermetadatapb.TableGroupProvisionClaim{
			Provisioner: provisioner, TablegroupId: id, TablegroupName: "commerce",
		}))
}

func (f *dynamicFixture) claimed(id int64) *clustermetadatapb.TableGroupProvisionClaim {
	f.t.Helper()
	c, err := f.topo.GetTableGroupProvisionClaim(f.t.Context(), "postgres", id)
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		return nil
	}
	require.NoError(f.t, err)
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

	require.NotNil(t, f.tableGroup("commerce"), "commerce not provisioned")
	c := f.claimed(7)
	require.NotNil(t, c, "provisioned without a claim")
	assert.Equal(t, provisionerID(f.cluster), c.GetProvisioner())
	assert.Equal(t, "commerce", c.GetTablegroupName())
	assert.Equal(t, 2, f.calls, "want the list and a re-read after claiming")

	// Converged: nothing pending, no further claim or re-read.
	f.calls = 0
	f.reconcile()
	assert.Equal(t, 1, f.calls, "a converged pass reads multiadmin once")
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

			assert.Nil(t, f.tableGroup("commerce"),
				"provisioned although the re-read no longer lists it as creating")
			assert.Nil(t, f.claimed(7), "claim left behind")
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

	assert.Nil(t, f.tableGroup("commerce"), "provisioned without a successful re-read")
	assert.NotNil(t, f.claimed(7),
		"claim released on a failed read: the catalog's state is unknown")
	c := meta.FindStatusCondition(f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups)
	if assert.NotNil(t, c) {
		assert.Equal(t, "DesiredStateUnknown", c.Reason)
	}

	// The next read lists it dropping and nothing was provisioned.
	f.reconcile()
	assert.Nil(t, f.claimed(7), "claim of a dropped id with nothing provisioned not released")
}

func TestClaimAndRecheck_AFailedRecheckIsNamedInTheError(t *testing.T) {
	cause := errors.New("HTTP 503")
	f := newDynamicFixture(t, true, func(string) ([]multiadmin.TableGroup, error) {
		return nil, cause
	})
	claims := &claimStore{r: f.r, cluster: f.cluster}
	defer claims.close()

	ready, problems, err := f.r.claimAndRecheck(t.Context(), f.cluster, claims, "postgres",
		commerceIn(multiadmin.StateCreating, 7))
	require.ErrorIs(t, err, cause)
	assert.ErrorContains(t, err,
		"failed to recheck the tablegroups of database postgres after claiming")
	assert.Empty(t, ready)
	assert.Equal(t, []string{"postgres: HTTP 503"}, problems)
	assert.NotNil(t, f.claimed(7), "claim released on a failed recheck")
}

func TestDynamicTableGroups_ClaimHeldElsewhereProvisionsNothing(t *testing.T) {
	f := newDynamicFixture(t, true, desiredCommerce(7))
	f.claim(7, "someone-else")
	f.reconcile()

	assert.Nil(t, f.tableGroup("commerce"), "provisioned an id another provisioner holds")
	assert.Equal(t, "someone-else", f.claimed(7).GetProvisioner(),
		"another provisioner's claim was replaced")
	assert.True(t, meta.IsStatusConditionFalse(f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups),
		"condition does not report the failed claim")
}

func TestDynamicTableGroups_DroppingTearsDownThenReleases(t *testing.T) {
	f := newDynamicFixture(t, true, listSequence(commerceIn(multiadmin.StateDropping, 7)),
		existingDynamic("test-cluster", "commerce", "7"))
	f.claim(7, provisionerID(f.cluster))

	f.reconcile()
	require.True(t, pendingDeletion(f.tableGroup("commerce")),
		"a tablegroup the catalog is dropping was not marked for deletion")
	require.NotNil(t, f.claimed(7), "claim released while the TableGroup exists")

	// The TableGroup is gone; one Shard is still running its finalizer.
	require.NoError(t, f.c.Delete(t.Context(), f.tableGroup("commerce")))
	shard := dynamicShard("7")
	require.NoError(t, f.c.Create(t.Context(), shard))
	f.reconcile()
	require.NotNil(t, f.claimed(7), "claim released while a Shard carrying the id exists")

	require.NoError(t, f.c.Delete(t.Context(), shard))
	f.reconcile()
	assert.Nil(t, f.claimed(7), "claim not released once nothing carries the id")
	// Idempotent: the catalog may list it a while longer.
	f.reconcile()
}

func TestDynamicTableGroups_DroppingLeavesAnotherProvisionersClaim(t *testing.T) {
	f := newDynamicFixture(t, true, listSequence(commerceIn(multiadmin.StateDropping, 7)))
	f.claim(7, "someone-else")
	f.reconcile()
	assert.NotNil(t, f.claimed(7), "released a claim another provisioner holds")
	p, _ := newPoller(f)
	assert.False(t, p.clusterNeedsReconcile(t.Context(), f.cluster),
		"another provisioner's claim asked for a reconcile")
}

func TestDynamicTableGroups_DroppingAnotherIDLeavesTheTableGroup(t *testing.T) {
	f := newDynamicFixture(t, true, listSequence(commerceIn(multiadmin.StateDropping, 9)),
		existingDynamic("test-cluster", "commerce", "7"), dynamicShard("7"))
	f.claim(7, provisionerID(f.cluster))
	f.claim(9, provisionerID(f.cluster))
	f.reconcile()

	assert.False(t, pendingDeletion(f.tableGroup("commerce")),
		"dropping id 9 marked the TableGroup of id 7")
	assert.NotNil(t, f.claimed(7), "claim of id 7 released")
	assert.Nil(t, f.claimed(9), "claim of id 9, with nothing carrying it, not released")
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

	assert.False(t, pendingDeletion(f.tableGroup("commerce")),
		"a tablegroup in an unknown state was removed")
	assert.Nil(t, f.tableGroup("orders"), "a tablegroup with no state was provisioned")
}

func TestDynamicTableGroups_GateOffDropsNothing(t *testing.T) {
	f := newDynamicFixture(t, false, listSequence(commerceIn(multiadmin.StateDropping, 7)),
		existingDynamic("test-cluster", "commerce", "7"))
	f.reconcile()
	assert.False(t, pendingDeletion(f.tableGroup("commerce")),
		"removed a tablegroup with the opt-in off")
}

func TestPoller_DroppedTableGroups(t *testing.T) {
	f := newDynamicFixture(t, true, listSequence(commerceIn(multiadmin.StateDropping, 7)),
		existingDynamic("test-cluster", "commerce", "7"))
	f.claim(7, provisionerID(f.cluster))
	p, _ := newPoller(f)
	ctx := t.Context()

	require.True(t, p.clusterNeedsReconcile(ctx, f.cluster),
		"a dropped tablegroup still serving did not ask for a reconcile")
	f.reconcile()
	require.False(t, p.clusterNeedsReconcile(ctx, f.cluster),
		"a draining tablegroup asked for a reconcile; its own events drive it")

	require.NoError(t, f.c.Delete(ctx, f.tableGroup("commerce")))
	require.True(t, p.clusterNeedsReconcile(ctx, f.cluster),
		"a held claim with no TableGroup left did not ask for a reconcile")
	f.reconcile()
	require.Nil(t, f.claimed(7), "claim not released")
	require.False(t, p.clusterNeedsReconcile(ctx, f.cluster),
		"a released drop asked for a reconcile")
}
