package toposerver

import (
	"cmp"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/types"
)

type grantFingerprint [sha256.Size]byte

// fingerprint hashes everything that decides an identity's etcd state.
func (g etcdIdentityGrant) fingerprint() grantFingerprint {
	permissions := slices.SortedFunc(slices.Values(g.Permissions), func(a, b etcdPermission) int {
		return cmp.Or(
			strings.Compare(a.Key, b.Key),
			strings.Compare(a.RangeEnd, b.RangeEnd),
			cmp.Compare(a.Type, b.Type),
		)
	})
	var b strings.Builder
	fmt.Fprintf(&b, "%q", g.Name)
	for _, p := range permissions {
		fmt.Fprintf(&b, "|%q,%q,%d", p.Key, p.RangeEnd, p.Type)
	}
	return sha256.Sum256([]byte(b.String()))
}

// verifiedServer holds the identities verified against one etcd cluster.
// Pairing the TopoServer UID with the cluster ID in one entry means a replaced
// TopoServer or a reset etcd both start from nothing.
type verifiedServer struct {
	name       types.NamespacedName
	clusterID  uint64
	identities map[string]grantFingerprint
}

// authVerifyCache remembers, in memory only, which identity grants were last
// fully reconciled. The zero value is ready to use and safe for concurrent use.
// A nil cache remembers nothing.
type authVerifyCache struct {
	mu      sync.Mutex
	servers map[types.UID]*verifiedServer
}

// sync reconciles the cache with what the pass just read from etcd. It drops
// everything for a server when auth is off or the cluster ID changed, and drops
// identities that are no longer desired, which covers pruned and frozen ones.
// A zero cluster ID counts as a change, so a missing header never caches.
func (c *authVerifyCache) sync(
	name types.NamespacedName,
	uid types.UID,
	clusterID uint64,
	enabled bool,
	desired map[string]bool,
) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for other, s := range c.servers {
		if s.name == name && other != uid {
			delete(c.servers, other)
		}
	}
	s := c.servers[uid]
	if s == nil || !enabled || clusterID == 0 || s.clusterID != clusterID {
		if c.servers == nil {
			c.servers = map[types.UID]*verifiedServer{}
		}
		s = &verifiedServer{
			name:       name,
			clusterID:  clusterID,
			identities: map[string]grantFingerprint{},
		}
		c.servers[uid] = s
	}
	for identity := range s.identities {
		if !desired[identity] {
			delete(s.identities, identity)
		}
	}
}

// applied reports whether the identity was fully reconciled with this grant.
// Entries never expire, so a change made directly in etcd stays until the grant
// changes, the entry is dropped, or the operator restarts.
func (c *authVerifyCache) applied(uid types.UID, identity string, fp grantFingerprint) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.servers[uid]
	if s == nil {
		return false
	}
	got, ok := s.identities[identity]
	return ok && got == fp
}

func (c *authVerifyCache) record(uid types.UID, identity string, fp grantFingerprint) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.servers[uid]; s != nil {
		s.identities[identity] = fp
	}
}

func (c *authVerifyCache) forget(uid types.UID, identity string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.servers[uid]; s != nil {
		delete(s.identities, identity)
	}
}

// evict drops every entry for a TopoServer that is gone or no longer managed.
func (c *authVerifyCache) evict(name types.NamespacedName) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for uid, s := range c.servers {
		if s.name == name {
			delete(c.servers, uid)
		}
	}
}
