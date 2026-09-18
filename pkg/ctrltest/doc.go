// Package ctrltest is a multi-controller envtest harness: one manager running
// every reconciler a consumer wants to run at once, so behaviour that is a
// protocol between controllers becomes testable.
//
// It knows nothing about any particular operator. Everything specific to one
// arrives through Options, and the consumer's reconcilers are wired in its own
// Register callback.
//
// # Assertions are closed-world by default
//
// A Script step declares exactly which changes it permits and fails on
// anything else. The failure this harness exists to prevent is an assertion
// that passed because it checked that something expected happened rather than
// that it was all that happened, so every event the stream delivers must be
// permitted by the step in flight. There is no mode, flag or option that
// relaxes that, and none may be added: a step that needs one is a step whose
// declaration is wrong.
//
// # An exact step is only as sound as the count behind it
//
// A step's allow-list is an exact multiset, with no "N events of this kind"
// wildcard and no intention to add one. So an exact step is sound only over
// writes whose number follows from the operator's code, and is unsound over
// any progression a concurrent actor paces. Declared against a paced
// progression it flakes on the harness rather than on the operator, which is
// worse than not writing the assertion at all, because it spends a reader's
// trust on a number nobody controls. Read those, or use RequireQuiescent for
// them. Script's own doc comment carries the two measured instances.
//
// # Every script must end with Finish
//
// A step's settle window can afford to be short because a straggler later than
// it lands in the next step and fails there, but the last step has no
// successor: without Finish the end of every script is open world, since the
// stream is shut down at cleanup and whatever it was holding is discarded.
// Forgetting Finish is therefore a failure rather than a pass, reported by a
// backstop registered on t.Cleanup. The only thing that closes a script
// without it is abandon, which exists for this package's own tests, where a
// script is deliberately left mid-failure.
//
// # KnownDefect pins rather than suppresses
//
// KnownDefect(t, ref, check) records a live defect: check returns a non-nil
// error while the defect is present, the test passes and logs, and the day the
// defect is fixed check returns nil and the test fails, telling the author to
// delete the pin and assert the correct behaviour instead. A pin is a claim
// with an expiry. A pin whose check cannot ever return nil is a suppression,
// not a claim, whatever it is called.
//
// # What the harness cannot see
//
// Rejected writes are invisible to the op log and to the matchers built on it.
// OpsInNamespace and Cursor are accepted-only by design, so that no assertion
// about what the operator did can be satisfied by a write the API server
// refused. Quiescence is the one measurement that widens to attempts, through
// attemptCursor and Op.Err, because a controller wedged on a refused write is
// exactly the kind of "something is happening" a quiet verdict must not miss.
//
// A Quiet() step cannot see residue that is an absence. Quiet asserts that
// nothing happened, and an orphan created once and never touched again emits
// nothing: no event, no write, no churn. Catching that needs a read of the
// namespace's contents, not a declaration about its event stream.
//
// # The reconciler seam is a contract, not a requirement
//
// The consumer wires the seam in its own Register callback, through
// s.Ops.For(name, client) for attribution and s.Reconciles.Wrap(name,
// reconciler) for the reconcile boundary. A controller that cannot accept a
// substituted reconciler still gets the recorder and the data plane simulator;
// what it does not get is namespace gating, requeue compression, and the
// reconcile records that say a controller ran and decided to do nothing.
//
// # The consumer owns the suite variable
//
// This package declares none. Boot returns a *Suite and a teardown func, and
// where they are kept, typically a package-level variable set from TestMain,
// is the consumer's business.
//
// # envtest runs no data plane
//
// There is no kubelet, no Deployment or StatefulSet controller and no volume
// provisioner behind an envtest API server. Nothing ever becomes Ready on its
// own. DataPlaneSim exists to fill exactly that gap, and it is also why
// relations.go reads relationships off the objects rather than recomputing the
// names the operator would have generated: recomputing proves only that a
// naming scheme is consistent with itself.
package ctrltest
