//go:build integration
// +build integration

package ownership_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/multigres/testkit/managedfields"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

func newEnvClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths(
		filepath.Join("../../", "config", "crd", "bases"),
	))
	return testutil.SetUpClient(t, cfg, scheme)
}

func newShard(name string) *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "0",
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Pgctld: "info", Multipooler: "info", Multiorch: "info",
				Multiadmin: "info", Multigateway: "info",
			},
			Images: multigresv1alpha1.ShardImages{
				Multiorch:   "ghcr.io/multigres/multigres:main",
				Multipooler: "ghcr.io/multigres/multigres:main",
				Postgres:    "postgres:17",
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			Multiorch: multigresv1alpha1.MultiorchSpec{
				Cells: []multigresv1alpha1.CellName{"zone-a"},
			},
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: "multigres-admin-password",
				Key:  "password",
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone-a"},
					Type:            "readWrite",
					ReplicasPerCell: ptr.To(int32(1)),
					Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
				},
			},
		},
	}
}

// seedStatusPhase records manager as an applier of the Shard's status, the way
// a status writer's server-side apply does.
func seedStatusPhase(ctx context.Context, c client.Client, name, manager string) error {
	shard := newShard(name)
	shard.Status.Phase = multigresv1alpha1.PhaseHealthy
	shard.Status.OrchReady = true
	shard.Status.PoolsReady = true
	return managedfields.Seed(ctx, c, shard, manager, managedfields.OnStatus())
}

func hasEntry(t *testing.T, shard *multigresv1alpha1.Shard, manager, subresource string) bool {
	t.Helper()
	entries, err := managedfields.Entries(shard)
	require.NoError(t, err)
	return slices.ContainsFunc(entries, func(e managedfields.Entry) bool {
		return e.Manager == manager && e.Subresource == subresource
	})
}

func TestProbe_SameValueApplySharesOwnership(t *testing.T) {
	c := newEnvClient(t)
	ctx := context.Background()

	shard := newShard("probe-same-value")
	require.NoError(t, c.Create(ctx, shard))
	require.NoError(t, seedStatusPhase(ctx, c, shard.Name, "probe-legacy"))
	require.NoError(t, seedStatusPhase(ctx, c, shard.Name, "probe-new"))

	got := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))

	require.True(t, hasEntry(t, got, "probe-legacy", "status"),
		"managedFields: %+v", got.GetManagedFields())
	require.True(t, hasEntry(t, got, "probe-new", "status"),
		"managedFields: %+v", got.GetManagedFields())
	require.Equal(t, []string{"probe-legacy", "probe-new"},
		managedfields.Owners(got, ".status.phase"),
		"managedFields: %+v", got.GetManagedFields())
}

func TestProbe_RemoveStatusEntryViaMainResource(t *testing.T) {
	c := newEnvClient(t)
	ctx := context.Background()

	shard := newShard("probe-remove")
	require.NoError(t, c.Create(ctx, shard))
	require.NoError(t, seedStatusPhase(ctx, c, shard.Name, "probe-legacy"))
	require.NoError(t, seedStatusPhase(ctx, c, shard.Name, "probe-new"))

	got := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
	idx := -1
	for i, e := range got.GetManagedFields() {
		if e.Manager == "probe-legacy" && e.Subresource == "status" {
			idx = i
		}
	}
	require.GreaterOrEqual(t, idx, 0)
	before := len(got.GetManagedFields())

	type op struct {
		Op    string `json:"op"`
		Path  string `json:"path"`
		Value any    `json:"value,omitempty"`
	}
	body, err := json.Marshal([]op{
		{Op: "test", Path: "/metadata/resourceVersion", Value: got.GetResourceVersion()},
		{
			Op:    "test",
			Path:  "/metadata/managedFields/" + strconv.Itoa(idx) + "/manager",
			Value: "probe-legacy",
		},
		{Op: "remove", Path: "/metadata/managedFields/" + strconv.Itoa(idx)},
	})
	require.NoError(t, err)
	require.NoError(t, c.Patch(ctx, got, client.RawPatch(types.JSONPatchType, body)))

	after := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), after))
	require.False(t, hasEntry(t, after, "probe-legacy", "status"),
		"legacy entry survived: %+v", after.GetManagedFields())
	require.True(t, hasEntry(t, after, "probe-new", "status"))
	require.Len(t, after.GetManagedFields(), before-1,
		"the patching client must not add an entry: %+v", after.GetManagedFields())
}

func TestDropLegacy_RemovesOnlyLegacy(t *testing.T) {
	c := newEnvClient(t)
	ctx := context.Background()

	shard := newShard("drop-legacy")
	require.NoError(t, c.Create(ctx, shard, client.FieldOwner(ownership.LegacyOperator)))
	require.NoError(
		t,
		seedStatusPhase(ctx, c, shard.Name, ownership.LegacyResourceHandlerReadiness),
	)
	require.NoError(t, seedStatusPhase(ctx, c, shard.Name, ownership.ManagerShardReadiness))

	before := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), before))
	var legacyBefore []string
	for _, m := range managedfields.Managers(before) {
		if ownership.IsLegacy(m) {
			legacyBefore = append(legacyBefore, m)
		}
	}
	require.Equal(t,
		[]string{ownership.LegacyOperator, ownership.LegacyResourceHandlerReadiness},
		legacyBefore,
		"main-resource and status legacy entries: %+v", before.GetManagedFields())

	require.NoError(t, ownership.DropLegacy(ctx, c, before))

	after := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), after))
	require.Len(t, after.GetManagedFields(), 1, "%+v", after.GetManagedFields())
	require.True(t, hasEntry(t, after, ownership.ManagerShardReadiness, "status"))
	require.Equal(t, before.Spec, after.Spec)
	require.Equal(t, before.Status, after.Status)
}

func TestDropLegacy_StaleObjectIsRejected(t *testing.T) {
	// Only the resourceVersion moves, so its test op is the sole guard.
	staleBy := map[string]func(ctx context.Context, c client.Client, name string) error{
		"label": func(ctx context.Context, c client.Client, name string) error {
			s := &multigresv1alpha1.Shard{}
			if err := c.Get(
				ctx,
				client.ObjectKey{Namespace: "default", Name: name},
				s,
			); err != nil {
				return err
			}
			base := s.DeepCopy()
			s.Labels = map[string]string{"touched": "yes"}
			return c.Patch(ctx, s, client.MergeFrom(base), client.FieldOwner("probe-other"))
		},
	}
	for name, makeStale := range staleBy {
		t.Run(name, func(t *testing.T) {
			c := newEnvClient(t)
			ctx := context.Background()

			shard := newShard("drop-legacy-stale-" + name)
			require.NoError(t, c.Create(ctx, shard))
			require.NoError(
				t,
				seedStatusPhase(ctx, c, shard.Name, ownership.LegacyResourceHandlerReadiness),
			)
			require.NoError(
				t,
				seedStatusPhase(ctx, c, shard.Name, ownership.ManagerShardReadiness),
			)

			stale := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), stale))
			require.NoError(t, makeStale(ctx, c, shard.Name))

			err := ownership.DropLegacy(ctx, c, stale)
			require.True(t, apierrors.IsInvalid(err), "want the guard's 422, got %v", err)

			after := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), after))
			require.True(
				t,
				hasEntry(t, after, ownership.LegacyResourceHandlerReadiness, "status"),
				"stale patch must not remove anything: %+v",
				after.GetManagedFields(),
			)
		})
	}
}
