package toposerver

import (
	"fmt"
	"sync"
	"testing"
	"time"

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
	now    time.Time
}

func newVerifyHarness(t *testing.T) *verifyHarness {
	h := &verifyHarness{
		t:      t,
		server: types.NamespacedName{Namespace: "stores", Name: "shared"},
		uid:    "uid-1",
	}
	h.now = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.cache = &authVerifyCache{now: func() time.Time { return h.now }}
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

func TestVerifiedPassRepairsDriftAfterIntervalOrChange(t *testing.T) {
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

	t.Run("unchanged grant is not repaired before the interval", func(t *testing.T) {
		h, c, grants, victim := setup(t)
		h.now = h.now.Add(authReverifyInterval - authReverifyJitter - time.Second)
		h.pass(c, grants)
		require.Len(t, c.roles[victim], 2, "the role changed behind the operator stays")
		require.Equal(t, fixedRoleGets, c.roleGets)
		require.Empty(t, c.mutations)
	})
	t.Run("unchanged grant is repaired after the interval", func(t *testing.T) {
		h, c, grants, victim := setup(t)
		h.now = h.now.Add(authReverifyInterval)
		h.pass(c, grants)
		require.Len(t, c.roles[victim], 1, "drift is revoked")
		require.Equal(t, fixedRoleGets+2*len(grants), c.roleGets)

		c.resetCounts()
		h.pass(c, grants)
		require.Equal(t, fixedRoleGets, c.roleGets, "the repaired grants are trusted again")
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

func TestVerifiedPassDropsPrunedIdentities(t *testing.T) {
	h := newVerifyHarness(t)
	c := newVerifiedEtcd()
	grants := numberedGrants(2)
	h.pass(c, grants)

	pruned := grants[0]
	_, err := reconcileEtcdAuth(
		t.Context(),
		c,
		grants[1:],
		[]string{pruned.Name, grants[1].Name},
		authPassOptions{
			continueOnError: true,
			verified:        h.cache,
			server:          h.server,
			uid:             h.uid,
		},
	)
	require.NoError(t, err)

	require.False(t, h.cached(pruned.Name, grants), "pruned identity loses its entry")
	require.True(t, h.cached(grants[1].Name, grants))
	require.NotContains(t, c.roles, pruned.Name)
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

	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	auth.resetCounts()

	ready, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, fixedRoleGets, auth.roleGets)
	require.Equal(t, fixedUserGets, auth.userGets)
}

func TestVerifiedCacheEntriesExpire(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := &authVerifyCache{now: func() time.Time { return now }}
	server := types.NamespacedName{Namespace: "stores", Name: "shared"}
	const identity = "/multigres/project"
	c.sync(server, "uid", 7, true, map[string]bool{identity: true})
	fp := prefixGrant(identity, nil, identity+"/global/").fingerprint()
	c.record("uid", identity, fp)
	start := now

	require.True(t, c.applied("uid", identity, fp), "a fresh entry is applied")
	now = start.Add(reverifyAfter(identity) - time.Nanosecond)
	require.True(t, c.applied("uid", identity, fp), "an entry is applied until it expires")
	now = start.Add(reverifyAfter(identity))
	require.False(t, c.applied("uid", identity, fp), "an expired entry is re-verified")

	c.record("uid", identity, fp)
	require.True(t, c.applied("uid", identity, fp), "re-recording refreshes the entry")
}

func TestVerifiedCacheJitterSpreadsExpiry(t *testing.T) {
	require.LessOrEqual(t, authReverifyInterval, maintenanceInterval,
		"the hourly requeue must find every entry expired")
	grants := numberedGrants(1000)
	expiries := map[time.Duration]bool{}
	expiredAtMidpoint := 0
	midpoint := authReverifyInterval - authReverifyJitter/2
	for _, grant := range grants {
		after := reverifyAfter(grant.Name)
		require.Greater(t, after, authReverifyInterval-authReverifyJitter)
		require.LessOrEqual(t, after, authReverifyInterval)
		require.Equal(t, after, reverifyAfter(grant.Name), "the offset is stable")
		expiries[after] = true
		if after <= midpoint {
			expiredAtMidpoint++
		}
	}
	require.Greater(t, len(expiries), 900, "identities expire at distinct times")
	require.InDelta(t, len(grants)/2, expiredAtMidpoint, float64(len(grants))/5,
		"about half the identities expire in the first half of the window")
}
