//go:build integration
// +build integration

package ownership_test

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/multigres/testkit/managedfields"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

// A status apply under the per-controller name, and the tidy-up after it,
// retire the legacy status entries (a retired facet name, and the child
// writer's name on status) and keep the child writer's main-resource entry,
// which is live even when its controller (the parent that applies the spec)
// has not run. No value changes.
func TestApplyStatus_RetiresOnlyLegacyEntries(t *testing.T) {
	c := newEnvClient(t)
	ctx := t.Context()

	shard := newShard("retire-legacy")
	require.NoError(t, managedfields.Seed(ctx, c, shard, ownership.ManagerChildWriter))
	require.NoError(t, seedStatusPhase(ctx, c, shard.Name, ownership.ManagerChildWriter))
	require.NoError(t,
		seedStatusPhase(ctx, c, shard.Name, ownership.LegacyResourceHandlerReadiness))

	seeded := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), seeded))
	require.Equal(
		t,
		[]string{ownership.ManagerChildWriter, ownership.LegacyResourceHandlerReadiness},
		legacyManagers(seeded),
		"%+v",
		seeded.GetManagedFields(),
	)
	require.True(t, hasEntry(t, seeded, ownership.ManagerChildWriter, ""))

	apply := newShard(shard.Name)
	apply.TypeMeta = metav1.TypeMeta{
		APIVersion: multigresv1alpha1.GroupVersion.String(),
		Kind:       "Shard",
	}
	apply.Spec = multigresv1alpha1.ShardSpec{}
	apply.Status = seeded.Status
	require.NoError(t, ownership.ApplyStatus(ctx, c, apply, ownership.ManagerShard))

	after := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), after))
	require.Empty(t, legacyManagers(after), "%+v", after.GetManagedFields())
	require.True(t, hasEntry(t, after, ownership.ManagerChildWriter, ""),
		"main-resource entry dropped: %+v", after.GetManagedFields())
	require.True(t, hasEntry(t, after, ownership.ManagerShard, "status"))
	require.Equal(t, seeded.Spec, after.Spec)
	require.Equal(t, seeded.Status, after.Status)
}

func legacyManagers(obj client.Object) []string {
	var out []string
	for _, e := range obj.GetManagedFields() {
		if ownership.IsLegacyEntry(e) {
			out = append(out, e.Manager)
		}
	}
	slices.Sort(out)
	return out
}

// A legacy entry that alone owns a field stays: removing it would leave the
// field populated with no owner.
func TestDropLegacy_KeepsSoleOwner(t *testing.T) {
	c := newEnvClient(t)
	ctx := t.Context()

	shard := newShard("sole-owner")
	require.NoError(t, c.Create(ctx, shard))
	require.NoError(t, seedStatusPhase(ctx, c, shard.Name, ownership.ManagerShard))
	legacy := newShard(shard.Name)
	legacy.Status.Phase = multigresv1alpha1.PhaseHealthy
	legacy.Status.OrchReady = true
	legacy.Status.PoolsReady = true
	legacy.Status.Message = "only the legacy entry sent this"
	require.NoError(t, managedfields.Seed(ctx, c, legacy,
		ownership.ManagerChildWriter, managedfields.OnStatus()))

	got := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
	require.NoError(t, ownership.DropLegacy(ctx, c, got))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
	require.Equal(t, []string{ownership.ManagerChildWriter},
		managedfields.Owners(got, ".status.message"),
		"the field's only owner was removed: %+v", got.GetManagedFields())

	// Once a current manager co-owns it, the legacy entry goes.
	require.NoError(t, managedfields.Seed(ctx, c, legacy,
		ownership.ManagerShard, managedfields.OnStatus()))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
	require.NoError(t, ownership.DropLegacy(ctx, c, got))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
	require.Empty(t, legacyManagers(got), "%+v", got.GetManagedFields())
}

// The Shard's merge-patch writers leave Update entries that claim the map
// container as well as its keys. A current writer's claim on every key covers
// the container, so the legacy entry goes; a key only the legacy entry claims
// keeps it.
func TestDropLegacy_UpdateEntryCoveredByKeys(t *testing.T) {
	c := newEnvClient(t)
	ctx := t.Context()

	shard := newShard("update-entry")
	require.NoError(t, c.Create(ctx, shard))
	require.NoError(t, seedStatusPhase(ctx, c, shard.Name, ownership.ManagerShard))
	setRoles := func(manager string, roles map[string]string) {
		t.Helper()
		cur := &multigresv1alpha1.Shard{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), cur))
		base := cur.DeepCopy()
		cur.Status.PodRoles = roles
		require.NoError(t, c.Status().Patch(ctx, cur, client.MergeFrom(base),
			client.FieldOwner(manager)))
	}
	setRoles(
		ownership.LegacyResourceHandler,
		map[string]string{"pod-0": "primary", "pod-1": "replica"},
	)
	setRoles(ownership.ManagerShard, map[string]string{"pod-0": "replica", "pod-1": "replica"})

	got := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
	require.Equal(t, []string{ownership.LegacyResourceHandler}, legacyManagers(got),
		"pod-1 kept its value, so the legacy entry still claims it: %+v", got.GetManagedFields())
	require.NoError(t, ownership.DropLegacy(ctx, c, got))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
	require.Equal(t, []string{ownership.LegacyResourceHandler}, legacyManagers(got),
		"pod-1 is claimed only by the legacy entry, which must stay")

	setRoles(ownership.ManagerShard, map[string]string{"pod-0": "replica", "pod-1": "primary"})
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
	before := got.DeepCopy()
	require.NoError(t, ownership.DropLegacy(ctx, c, got))
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
	require.Empty(t, legacyManagers(got), "%+v", got.GetManagedFields())
	require.Equal(t, before.Status, got.Status)
}

// A container claim is not a claim on what is below it. The apiserver records
// the remaining container of a granular map, and an applied empty object, as
// a leaf with nothing below it: exactly what an atomic list looks like. Taking
// either as covering a key or field would remove the legacy entry that is its
// only owner.
func TestDropLegacy_ContainerClaimCoversNothingBelowIt(t *testing.T) {
	c := newEnvClient(t)
	ctx := t.Context()
	key := func(s *multigresv1alpha1.Shard) client.ObjectKey { return client.ObjectKeyFromObject(s) }

	t.Run("granular map container", func(t *testing.T) {
		shard := newShard("container-map")
		require.NoError(t, c.Create(ctx, shard))
		require.NoError(t, seedStatusPhase(ctx, c, shard.Name, ownership.ManagerShardReadiness))
		setRoles := func(manager, role string) {
			t.Helper()
			cur := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(ctx, key(shard), cur))
			base := cur.DeepCopy()
			cur.Status.PodRoles = map[string]string{"pod-0": role}
			require.NoError(t, c.Status().Patch(ctx, cur, client.MergeFrom(base),
				client.FieldOwner(manager)))
		}
		// The current writer sets the role, then an operator writing under
		// the shared name changes it: the current entry keeps only the
		// container, the legacy one owns the key.
		setRoles(ownership.ManagerShard, "replica")
		setRoles(ownership.LegacyResourceHandler, "primary")

		got := &multigresv1alpha1.Shard{}
		require.NoError(t, c.Get(ctx, key(shard), got))
		require.NoError(t, ownership.DropLegacy(ctx, c, got))
		require.NoError(t, c.Get(ctx, key(shard), got))
		require.Equal(t, "primary", got.Status.PodRoles["pod-0"])
		require.Equal(t, []string{ownership.LegacyResourceHandler},
			managedfields.Owners(got, ".status.podRoles.pod-0"),
			"the key's only owner was removed: %+v", got.GetManagedFields())
	})

	t.Run("applied empty object", func(t *testing.T) {
		shard := newShard("container-object")
		require.NoError(t, c.Create(ctx, shard))
		legacy := newShard(shard.Name)
		legacy.Status.OrchReady, legacy.Status.PoolsReady = true, true
		legacy.Status.PostgresConfig = &multigresv1alpha1.PostgresConfigStatus{Error: "boom"}
		require.NoError(t, managedfields.Seed(ctx, c, legacy, ownership.LegacyResourceHandler,
			managedfields.OnStatus()))

		apply := newShard(shard.Name)
		apply.TypeMeta = metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "Shard",
		}
		apply.Spec = multigresv1alpha1.ShardSpec{}
		apply.Status.OrchReady, apply.Status.PoolsReady = true, true
		apply.Status.PostgresConfig = &multigresv1alpha1.PostgresConfigStatus{}
		require.NoError(t, ownership.ApplyStatus(ctx, c, apply, ownership.ManagerShard))

		got := &multigresv1alpha1.Shard{}
		require.NoError(t, c.Get(ctx, key(shard), got))
		require.Equal(t, "boom", got.Status.PostgresConfig.Error)
		require.Equal(t, []string{ownership.LegacyResourceHandler},
			managedfields.Owners(got, ".status.postgresConfig.error"),
			"the field's only owner was removed: %+v", got.GetManagedFields())
	})
}

// DropLegacy reads managedFields, then patches by index with no
// resourceVersion precondition. A write between the two must either leave
// the drop correct or fail it with a 422; it must never remove anything but
// the legacy entry the read saw.
func TestDropLegacy_ConcurrentWrites(t *testing.T) {
	for name, tc := range map[string]struct {
		write     func(ctx context.Context, c client.Client, name string) error
		wantError bool
		// legacyUpdate seeds the legacy entry as an Update-operation status
		// write of message, and the current owner as an Apply of message too.
		legacyUpdate bool
	}{
		// Another manager writes the main resource: the busy-object case
		// (a kubelet, the Deployment controller). The drop still lands.
		"other manager adds an entry": {
			write: func(ctx context.Context, c client.Client, name string) error {
				s := &multigresv1alpha1.Shard{}
				if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, s); err != nil {
					return err
				}
				base := s.DeepCopy()
				s.Labels = map[string]string{"touched": "yes"}
				return c.Patch(ctx, s, client.MergeFrom(base), client.FieldOwner("probe-other"))
			},
		},
		// The current owner withdraws a field it shares with the legacy
		// entry. The field stays populated, owned by the legacy entry alone,
		// so removing that entry would strand it. The current entry's fields
		// are tested, so the patch fails instead. Apply entries sort before
		// Update entries, so the legacy Update entry keeps its index and its
		// own test still passes: only the covering entry's guard catches it.
		"current owner withdraws a shared field": {
			legacyUpdate: true,
			write: func(ctx context.Context, c client.Client, name string) error {
				s := newShard(name)
				s.Status.Phase = multigresv1alpha1.PhaseHealthy
				s.Status.OrchReady = true
				s.Status.PoolsReady = true
				return managedfields.Seed(ctx, c, s, ownership.ManagerShard,
					managedfields.OnStatus())
			},
			wantError: true,
		},
		// The legacy writer writes again (an operator writing under the shared
		// name during a rollback): the entry stored is not the entry read, so
		// its whole-entry test fails the patch.
		"legacy entry rewritten": {
			write: func(ctx context.Context, c client.Client, name string) error {
				s := newShard(name)
				s.Status.Phase = multigresv1alpha1.PhaseHealthy
				s.Status.OrchReady = true
				s.Status.PoolsReady = true
				s.Status.Message = "legacy again"
				return managedfields.Seed(ctx, c, s, ownership.LegacyResourceHandlerReadiness,
					managedfields.OnStatus())
			},
			wantError: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newEnvClient(t)
			ctx := t.Context()

			shard := newShard("concurrent")
			require.NoError(t, c.Create(ctx, shard))
			if tc.legacyUpdate {
				seedSharedMessage(t, c, shard.Name)
			} else {
				require.NoError(t,
					seedStatusPhase(ctx, c, shard.Name, ownership.LegacyResourceHandlerReadiness))
				require.NoError(t,
					seedStatusPhase(ctx, c, shard.Name, ownership.ManagerShardReadiness))
			}

			stale := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), stale))
			require.NoError(t, tc.write(ctx, c, shard.Name))
			mid := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), mid))

			err := ownership.DropLegacy(ctx, c, stale)

			after := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), after))
			if tc.wantError {
				require.ErrorIs(t, err, ownership.ErrManagedFieldsChanged)
				require.True(t, apierrors.IsInvalid(err), "want the guard's 422, got %v", err)
				require.Equal(t, mid.GetManagedFields(), after.GetManagedFields(),
					"a failed guard must remove nothing")
				if after.Status.Message != "" {
					require.NotEmpty(t, managedfields.Owners(after, ".status.message"),
						"a populated field was left without an owner: %+v",
						after.GetManagedFields())
				}
				return
			}
			require.NoError(t, err)
			var want []metav1.ManagedFieldsEntry
			for _, e := range mid.GetManagedFields() {
				if !ownership.IsLegacyEntry(e) {
					want = append(want, e)
				}
			}
			require.Equal(t, want, after.GetManagedFields(),
				"exactly the legacy entry must go")
		})
	}
}

// seedSharedMessage leaves the legacy status writer's Update entry and the
// current owner's Apply entry both claiming .status.message.
func seedSharedMessage(t *testing.T, c client.Client, name string) {
	t.Helper()
	ctx := t.Context()
	s := newShard(name)
	s.Status.Phase = multigresv1alpha1.PhaseHealthy
	s.Status.OrchReady = true
	s.Status.PoolsReady = true
	s.Status.Message = "current"
	require.NoError(t, managedfields.Seed(ctx, c, s, ownership.ManagerShard,
		managedfields.OnStatus()))
	cur := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, cur))
	base := cur.DeepCopy()
	cur.Status.Message = "shared"
	require.NoError(t, c.Status().Patch(ctx, cur, client.MergeFrom(base),
		client.FieldOwner(ownership.LegacyResourceHandler)))
	s.Status.Message = "shared"
	require.NoError(t, managedfields.Seed(ctx, c, s, ownership.ManagerShard,
		managedfields.OnStatus()))
}

// A tidy-up patch an admission check rejects (on a MultigresCluster, the
// webhook rejects every write once the spec does not validate) is a 422 too.
// A ValidatingAdmissionPolicy denial names the object in its details, so it is
// reported as an error, not as a changed list.
func TestDropLegacy_AdmissionDenialIsNotAChangedList(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, admissionregistrationv1.AddToScheme(scheme))
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths(
		filepath.Join("../../", "config", "crd", "bases"),
	))
	c := testutil.SetUpClient(t, cfg, scheme)
	ctx := t.Context()

	require.NoError(t, c.Create(ctx, &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "deny"},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			MatchConstraints: &admissionregistrationv1.MatchResources{
				ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
					RuleWithOperations: admissionregistrationv1.RuleWithOperations{
						Operations: []admissionregistrationv1.OperationType{
							admissionregistrationv1.Update,
						},
						Rule: admissionregistrationv1.Rule{
							APIGroups:   []string{multigresv1alpha1.GroupVersion.Group},
							APIVersions: []string{"*"},
							Resources:   []string{"shards"},
						},
					},
				}},
			},
			Validations: []admissionregistrationv1.Validation{{
				Expression: "false", Message: "spec does not validate",
			}},
		},
	}))
	require.NoError(t, c.Create(ctx, &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "deny"},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName: "deny",
			ValidationActions: []admissionregistrationv1.ValidationAction{
				admissionregistrationv1.Deny,
			},
		},
	}))

	shard := newShard("denied")
	require.NoError(t, c.Create(ctx, shard))
	require.NoError(t, seedStatusPhase(ctx, c, shard.Name, ownership.ManagerShardReadiness))
	// The policy takes effect asynchronously; until it does the drop succeeds,
	// so the legacy entry is seeded again before each attempt.
	var err error
	require.Eventually(t, func() bool {
		require.NoError(t, seedStatusPhase(ctx, c, shard.Name,
			ownership.LegacyResourceHandlerReadiness))
		got := &multigresv1alpha1.Shard{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
		err = ownership.DropLegacy(ctx, c, got)
		return err != nil
	}, 30*time.Second, 200*time.Millisecond)
	require.True(t, apierrors.IsInvalid(err), "want a 422, got %v", err)
	require.NotErrorIs(t, err, ownership.ErrManagedFieldsChanged)
}
