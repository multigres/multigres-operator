package suite

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/multigres/testkit/ctrltest"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// C is this operator's test context: the generic harness handle from
// ctrltest, plus the multigres vocabulary.
//
// A local type because Go cannot add methods to another package's, which is
// the point rather than a workaround. pkg/ctrltest is meant to be copied into
// other operators, so it carries assertions and harness pointers and knows
// nothing about shards or poolers; each consumer wraps it and hangs its own
// domain on the same receiver. Everything ctrltest offers is promoted, so
// c.NoError and c.WaitForClusterHealthy read alike at the call site.
type C struct {
	*ctrltest.C
	// legacy is the legacy-entry rule requireOwnership applies at cleanup.
	legacy legacyRule
	// tolerated are the misclaims requireOwnership lets this case pass.
	tolerated []misclaim
}

// misclaim is one status-ownership violation a case may tolerate: field, or
// any field below it, on an object of kind, owned by owner but claimed by
// claimer. Anything else, including the same field claimed by another
// manager, is still a violation.
type misclaim struct {
	kind, field, owner, claimer string
}

// matches reports whether violation, as checkOwnership formats it, is this
// misclaim.
func (m misclaim) matches(violation string) bool {
	rest, ok := strings.CutPrefix(violation, m.kind+" ")
	if !ok {
		return false
	}
	if _, rest, ok = strings.Cut(rest, ": status field "); !ok {
		return false
	}
	path, tail, ok := strings.Cut(rest, " owned by ")
	underField := path == m.field || strings.HasPrefix(path, m.field+".")
	return ok && underField && tail == m.owner+", claimed by "+m.claimer
}

// tolerateMisclaim lets this case's cleanup ownership check pass m, for a
// case whose own claim is unrelated to field ownership but whose scenario
// reliably reaches a known operator misclaim. Each tolerated violation is
// still logged. Remove the call once the operator stops making the misclaim.
func (c *C) tolerateMisclaim(m misclaim) {
	c.tolerated = append(c.tolerated, m)
}

// newCase opens a test context on its own namespace.
//
// Every test in this package should start with one. It allocates the
// namespace, activates the reconcile gate for it, and registers the failure
// dump, so a failing test prints the interleaved op log and reconcile records
// rather than only the assertion message.
func newCase(t *testing.T) *C {
	t.Helper()
	// Registered before Suite.Case so it runs last (cleanups are LIFO), after
	// testkit has closed the namespace's reconcile gate, so no controller
	// recreates what it deletes.
	var ns string
	t.Cleanup(func() { deleteServices(t, ns) })
	c := &C{C: Suite.Case(t)}
	ns = c.NS
	// Registered after Suite.Case so it runs first (cleanups are LIFO): before
	// testkit closes the namespace's reconcile gate and deletes the namespace.
	// It lives here rather than in testkit because every test in this package
	// funnels through newCase, so no suite-wide hook is needed.
	t.Cleanup(func() { c.requireOwnership(t) })
	return c
}

// requireOwnership fails the test with every field-ownership violation in its
// namespace. It runs even when the test already failed, since a violation is
// evidence for that failure. A case that created no CR has nothing to check,
// which is why this uses the lenient checkOwnership.
func (c *C) requireOwnership(t *testing.T) {
	t.Helper()
	violations, _, err := checkOwnership(context.Background(), Suite.Client, c.NS, c.legacy)
	if err != nil {
		t.Errorf("status ownership check: %v", err)
		return
	}
	violations = slices.DeleteFunc(violations, func(v string) bool {
		for _, m := range c.tolerated {
			if m.matches(v) {
				t.Logf("tolerated status ownership violation: %s", v)
				return true
			}
		}
		return false
	})
	if len(violations) > 0 {
		t.Errorf("status ownership violations in %s:\n  %s", c.NS,
			strings.Join(violations, "\n  "))
	}
}

// seedsLegacy marks a case that writes legacy entries on purpose, so its
// cleanup check allows the ones DropLegacy must keep. Every other case allows
// none.
func (c *C) seedsLegacy() {
	c.legacy = keptLegacy
}

// deleteServices frees the cluster IPs a finished case holds. envtest runs no
// namespace controller, so deleting a case's namespace never deletes the
// Services in it, and every case shares one apiserver whose service range
// (10.0.0.0/24 by default) the full package otherwise exhausts. testkit's
// ctrltest offers no way to widen that range or to model namespace deletion;
// this can go when it does either.
func deleteServices(t *testing.T, ns string) {
	if ns == "" {
		return
	}
	err := Suite.Client.DeleteAllOf(context.Background(), &corev1.Service{}, client.InNamespace(ns))
	if err != nil {
		t.Errorf("delete Services in %s: %v", ns, err)
	}
}

// newBareCase opens a test context with no namespace, for the tests in this
// package that are pure logic and never touch the cluster.
//
// identity_test.go is all of them: MembersOf and ShardPVCOf take objects and
// return answers. newCase would allocate a real namespace against envtest and
// register it at the reconcile gate for each one, which buys nothing.
func newBareCase(t *testing.T) *C {
	t.Helper()
	return &C{C: ctrltest.Bare(t)}
}

// Sub binds this case to a subtest's T while keeping its namespace. Use it
// for a t.Run that asserts about objects the parent test created; use newCase
// for a subtest that wants a namespace of its own.
//
// It shadows the embedded ctrltest.C.Sub so that one name always hands back
// this package's C, with the multigres vocabulary still on it. Without the
// shadow a subtest would silently drop to the generic type and lose every
// method below.
func (c *C) Sub(t *testing.T) *C {
	t.Helper()
	return &C{C: c.C.Sub(t)}
}

// Check returns a C whose assertions report and continue rather than abort,
// shadowed for the same reason as Sub: without it c.Check() hands back a
// *ctrltest.C and a collecting assertion silently loses every method below.
func (c *C) Check() *C {
	return &C{C: c.C.Check()}
}

// Assert at compile time that both shadows hand back this package's type. The
// regression they guard against is silent: dropping to *ctrltest.C still
// compiles at every existing call site, and only stops compiling once someone
// chains a multigres method off one of them.
var _ = func(c *C) (*C, *C) { return c.Check(), c.Sub(nil) }
