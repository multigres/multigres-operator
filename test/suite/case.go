package suite

import (
	"testing"

	"github.com/multigres/testkit/ctrltest"
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
}

// newCase opens a test context on its own namespace.
//
// Every test in this package should start with one. It allocates the
// namespace, activates the reconcile gate for it, and registers the failure
// dump, so a failing test prints the interleaved op log and reconcile records
// rather than only the assertion message.
func newCase(t *testing.T) *C {
	t.Helper()
	return &C{C: Suite.Case(t)}
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
