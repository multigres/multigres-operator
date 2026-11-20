package toposerver

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"k8s.io/apimachinery/pkg/types"
)

// The fixed cost of every pass is the root role and the root and operator users.
const (
	fixedRoleGets = 1
	fixedUserGets = 2
)

type verifyHarness struct {
	t      *testing.T
	cache  *authVerifyCache
	server types.NamespacedName
	uid    types.UID
}

func newVerifyHarness(t *testing.T) *verifyHarness {
	h := &verifyHarness{
		t:      t,
		server: types.NamespacedName{Namespace: "stores", Name: "shared"},
		uid:    "uid-1",
	}
	h.cache = &authVerifyCache{}
	return h
}

func newVerifiedEtcd() *fakeEtcdAuth {
	c := newFakeEtcdAuth()
	c.enabled, c.clusterID = true, 7
	return c
}

func (h *verifyHarness) pass(
	c *fakeEtcdAuth,
	desired []etcdIdentityGrant,
	previous ...string,
) authPassResult {
	h.t.Helper()
	result, err := reconcileEtcdAuth(h.t.Context(), c, desired, previous, authPassOptions{
		continueOnError: true,
		verified:        h.cache,
		server:          h.server,
		uid:             h.uid,
	})
	require.NoError(h.t, err)
	return result
}

func (h *verifyHarness) cached(identity string, grants []etcdIdentityGrant) bool {
	for _, g := range grants {
		if g.Name == identity {
			return h.cache.applied(h.uid, identity, g.fingerprint())
		}
	}
	return false
}

func numberedGrants(n int) []etcdIdentityGrant {
	grants := make([]etcdIdentityGrant, n)
	for i := range grants {
		name := fmt.Sprintf("/multigres/p%04d", i)
		grants[i] = prefixGrant(name, nil, name+"/global/")
	}
	return grants
}

func (f *fakeEtcdAuth) resetCounts() { f.roleGets, f.userGets, f.mutations = 0, 0, nil }

func TestVerifiedPassSkipsUnchangedIdentities(t *testing.T) {
	for _, n := range []int{1, 10, 100, 1000} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d identities cached=%t", n, cached), func(t *testing.T) {
				h := newVerifyHarness(t)
				if !cached {
					h.cache = nil
				}
				c := newVerifiedEtcd()
				grants := numberedGrants(n)
				h.pass(c, grants)
				c.resetCounts()
				h.pass(c, grants)

				wantRoles, wantUsers := fixedRoleGets, fixedUserGets
				if !cached {
					wantRoles += 2 * n
					wantUsers += n
				}
				require.Equal(t, wantRoles, c.roleGets, "RoleGet calls in a no-op pass")
				require.Equal(t, wantUsers, c.userGets, "UserGet calls in a no-op pass")
				require.Empty(t, c.mutations)
				t.Logf("N=%d cached=%t RoleGet=%d UserGet=%d", n, cached, c.roleGets, c.userGets)
			})
		}
	}
}

func TestVerifiedPassReappliesOnlyChangedGrant(t *testing.T) {
	h := newVerifyHarness(t)
	c := newVerifiedEtcd()
	grants := numberedGrants(5)
	h.pass(c, grants)
	c.resetCounts()

	name := grants[2].Name
	grants[2] = prefixGrant(name, nil, name+"/global/", name+"/extra/")
	h.pass(c, grants)

	require.Equal(t, fixedRoleGets+2, c.roleGets)
	require.Equal(t, fixedUserGets+1, c.userGets)
	require.Equal(t, []string{"permission:" + name + ":" + name + "/extra/"}, c.mutations)
	require.Len(t, c.roles[grants[2].Name], 2)
}

func TestVerifiedPassFingerprintIgnoresPermissionOrder(t *testing.T) {
	a := prefixGrant("id", nil, "/a/", "/b/")
	b := prefixGrant("id", nil, "/b/", "/a/")
	require.Equal(t, a.fingerprint(), b.fingerprint())
	require.NotEqual(t, a.fingerprint(), prefixGrant("id", nil, "/a/").fingerprint())
	require.NotEqual(t, a.fingerprint(), prefixGrant("other", nil, "/a/", "/b/").fingerprint())
	readOnly := prefixGrant("id", nil, "/a/")
	readOnly.Permissions[0].Type = authpb.READ
	require.NotEqual(t, prefixGrant("id", nil, "/a/").fingerprint(), readOnly.fingerprint())
}

func TestVerifiedPassDoesNotRepairDriftUntilGrantOrClusterChanges(t *testing.T) {
	stolen := &authpb.Permission{
		Key: []byte("/stolen/"), RangeEnd: []byte("/stolen0"), PermType: authpb.READWRITE,
	}
	setup := func(t *testing.T) (*verifyHarness, *fakeEtcdAuth, []etcdIdentityGrant, string) {
		h := newVerifyHarness(t)
		c := newVerifiedEtcd()
		grants := numberedGrants(3)
		h.pass(c, grants)
		victim := grants[1].Name
		c.roles[victim] = append(c.roles[victim], stolen)
		c.resetCounts()
		return h, c, grants, victim
	}

	t.Run("unchanged grant is not repaired", func(t *testing.T) {
		h, c, grants, victim := setup(t)
		h.pass(c, grants)
		require.Len(t, c.roles[victim], 2, "the role changed behind the operator stays")
		require.Equal(t, fixedRoleGets, c.roleGets)
		require.Empty(t, c.mutations)
	})
	t.Run("changed grant is repaired", func(t *testing.T) {
		h, c, grants, victim := setup(t)
		grants[1] = prefixGrant(victim, nil, victim+"/global/", victim+"/extra/")
		h.pass(c, grants)
		require.ElementsMatch(
			t,
			[]string{victim + "/global/", victim + "/extra/"},
			permissionKeys(c.roles[victim]),
		)
		require.Equal(t, fixedRoleGets+2, c.roleGets, "only the changed identity is read")
	})
	t.Run("cleared cache is repaired", func(t *testing.T) {
		h, c, grants, victim := setup(t)
		c.clusterID = 8
		h.pass(c, grants)
		require.Len(t, c.roles[victim], 1, "drift is revoked")
		require.Equal(t, fixedRoleGets+2*len(grants), c.roleGets)
	})
}

func permissionKeys(perms []*authpb.Permission) []string {
	keys := make([]string, len(perms))
	for i, p := range perms {
		keys[i] = string(p.Key)
	}
	return keys
}

func TestVerifiedPassClearsOnEtcdIdentityChange(t *testing.T) {
	for name, reset := range map[string]func(*fakeEtcdAuth){
		"different cluster ID": func(c *fakeEtcdAuth) { c.clusterID = 8 },
		"auth not enabled":     func(c *fakeEtcdAuth) { c.enabled = false },
		"header missing":       func(c *fakeEtcdAuth) { c.clusterID = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			h := newVerifyHarness(t)
			c := newVerifiedEtcd()
			grants := numberedGrants(3)
			h.pass(c, grants)
			c.roles[grants[0].Name] = nil
			reset(c)
			c.resetCounts()

			h.pass(c, grants)

			require.Len(t, c.roles[grants[0].Name], 1, "everything is verified again")
			require.Equal(t, fixedRoleGets+2*len(grants), c.roleGets)
			require.Equal(t, fixedUserGets+len(grants), c.userGets)
		})
	}
}

func TestVerifiedPassFailedIdentityIsRetried(t *testing.T) {
	h := newVerifyHarness(t)
	c := newVerifiedEtcd()
	grants := numberedGrants(3)
	h.pass(c, grants)
	bad := grants[1]
	require.True(t, h.cached(bad.Name, grants))

	bad.Permissions = append(bad.Permissions, etcdPrefixPermission(bad.Name+"/new/"))
	grants[1] = bad
	c.failRoles = map[string]bool{bad.Name: true}
	result := h.pass(c, grants)
	require.Contains(t, result.failed, bad.Name)
	require.False(t, h.cached(bad.Name, grants), "a failed identity has no entry")
	require.True(t, h.cached(grants[0].Name, grants))

	c.failRoles = nil
	c.resetCounts()
	result = h.pass(c, grants)
	require.Empty(t, result.failed)
	require.Equal(t, fixedRoleGets+2, c.roleGets, "only the failed identity is retried")
	require.Len(t, c.roles[bad.Name], 2)
	require.True(t, h.cached(bad.Name, grants))
}

func TestVerifiedPassNeverCachesActivationFailure(t *testing.T) {
	h := newVerifyHarness(t)
	c := newVerifiedEtcd()
	c.enabled, c.failGrant = false, true
	grants := numberedGrants(2)
	_, err := reconcileEtcdAuth(t.Context(), c, grants, nil, authPassOptions{
		verified: h.cache, server: h.server, uid: h.uid,
	})
	require.Error(t, err)
	require.False(t, h.cached(grants[0].Name, grants))
	require.False(t, h.cached(grants[1].Name, grants))
}

func TestVerifiedPassDropsPrunedAndFrozenIdentities(t *testing.T) {
	h := newVerifyHarness(t)
	c := newVerifiedEtcd()
	grants := numberedGrants(3)
	h.pass(c, grants)

	pruned, frozen := grants[0], grants[1]
	_, err := reconcileEtcdAuth(
		t.Context(),
		c,
		grants[2:],
		[]string{pruned.Name, frozen.Name, grants[2].Name},
		authPassOptions{
			continueOnError: true,
			retain:          map[string]bool{frozen.Name: true},
			verified:        h.cache,
			server:          h.server,
			uid:             h.uid,
		},
	)
	require.NoError(t, err)

	require.False(t, h.cached(pruned.Name, grants), "pruned identity loses its entry")
	require.False(t, h.cached(frozen.Name, grants), "frozen identity loses its entry")
	require.True(t, h.cached(grants[2].Name, grants))
	require.NotContains(t, c.roles, pruned.Name)
	require.Contains(t, c.roles, frozen.Name, "frozen identity keeps what it holds")
}

func TestVerifiedPassKeepsServersApart(t *testing.T) {
	h := newVerifyHarness(t)
	first, second := newVerifiedEtcd(), newVerifiedEtcd()
	grants := numberedGrants(2)
	h.pass(first, grants)

	h.uid = "uid-2"
	h.server = types.NamespacedName{Namespace: "stores", Name: "other"}
	h.pass(second, grants)

	require.Len(t, second.roles, len(grants)+1, "the second server is not trusted from the first")
	require.Equal(t, fixedRoleGets+2*len(grants), second.roleGets)

	h.uid = "uid-1"
	h.server = types.NamespacedName{Namespace: "stores", Name: "shared"}
	require.True(t, h.cached(grants[0].Name, grants), "the first server's entries are untouched")
}

func TestVerifiedCacheEviction(t *testing.T) {
	grants := numberedGrants(1)
	shared := types.NamespacedName{Namespace: "stores", Name: "shared"}
	other := types.NamespacedName{Namespace: "stores", Name: "other"}
	desired := map[string]bool{grants[0].Name: true}
	fill := func(c *authVerifyCache, name types.NamespacedName, uid types.UID) {
		c.sync(name, uid, 7, true, desired)
		c.record(uid, grants[0].Name, grants[0].fingerprint())
	}

	t.Run("recreated server replaces the old UID", func(t *testing.T) {
		c := &authVerifyCache{}
		fill(c, shared, "old")
		fill(c, other, "keep")
		c.sync(shared, "new", 7, true, desired)
		require.ElementsMatch(t, []types.UID{"new", "keep"}, mapKeys(c.servers))
	})
	t.Run("deleted server is evicted", func(t *testing.T) {
		c := &authVerifyCache{}
		fill(c, shared, "old")
		fill(c, other, "keep")
		c.evict(shared)
		require.ElementsMatch(t, []types.UID{"keep"}, mapKeys(c.servers))
	})
	t.Run("nil cache is inert", func(t *testing.T) {
		var c *authVerifyCache
		c.sync(shared, "x", 7, true, desired)
		c.record("x", "id", grantFingerprint{})
		c.forget("x", "id")
		c.evict(shared)
		require.False(t, c.applied("x", "id", grantFingerprint{}))
	})
}

func mapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestVerifiedCacheConcurrentPasses(t *testing.T) {
	cache := &authVerifyCache{}
	grants := numberedGrants(20)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := types.NamespacedName{Namespace: "stores", Name: fmt.Sprintf("ts-%d", i)}
			uid := types.UID(fmt.Sprintf("uid-%d", i))
			c := newVerifiedEtcd()
			for round := range 20 {
				desired := grants[:10+round%10]
				_, err := reconcileEtcdAuth(t.Context(), c, desired, nil, authPassOptions{
					continueOnError: true, verified: cache, server: name, uid: uid,
				})
				if err != nil {
					t.Error(err)
					return
				}
				if round%7 == 6 {
					cache.evict(name)
				}
			}
		}()
	}
	wg.Wait()
	require.Len(t, cache.servers, 8)
}

func TestTopologyAuthSkipsUnchangedIdentitiesAcrossPasses(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	auth.clusterID = 7
	r.authVerified = &authVerifyCache{}
	const identity = "/multigres/project"

	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	auth.resetCounts()

	ready, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, fixedRoleGets, auth.roleGets)
	require.Equal(t, fixedUserGets, auth.userGets)

	conflicting := fallbackRootCluster("project", "global")
	require.NoError(t, r.Create(t.Context(), conflicting))
	_, err = r.reconcileTopologyAuth(t.Context(), ts)
	var conflict *grantConflictError
	require.True(t, errors.As(err, &conflict), "got %v", err)
	require.Contains(t, auth.roles, identity, "frozen identity keeps its grant")
	require.Empty(
		t,
		r.authVerified.servers[ts.UID].identities,
		"frozen identities are never cached",
	)

	require.NoError(t, r.Delete(t.Context(), conflicting))
	auth.resetCounts()
	_, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.Equal(t, fixedRoleGets+2, auth.roleGets, "the thawed identity is verified once")
}
