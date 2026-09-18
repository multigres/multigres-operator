// KnownDefect exists so a test that has found a live operator defect can
// record it rather than land red. A red test in an advisory job trains people
// to ignore the job, so a known defect is pinned instead: the advisory CI job
// stays green while the defect is present, and the pin itself is temporary. On
// the day the defect is fixed, the same test flips red, which is the intended
// signal rather than noise, and the fix arrives as a commit that deletes the
// pin and replaces it with the positive assertion it was standing in for.
//
// A pin may also record behaviour that is awaiting a design ruling rather than
// a fix, and that variant expires differently: it expires when the ruling
// lands, either through the code change the ruling calls for or by being
// rewritten as a positive assertion of whatever the ruling declared correct.
// So "temporary" still holds, but the clock is a decision rather than a bug
// fix, and such a pin can outlive several releases while the decision is open
// without becoming a suppression. It is the executable form of the open
// question, and deleting it would erase the only artifact keeping the question
// visible. What is not allowed is a pin with no expiry condition at all: one
// whose own comment cannot say what would retire it is a suppression whatever
// it is called. See the two-member-pool pin in seam_shard_lifecycle_test.go,
// which enumerates three design changes any one of which retires it.

package ctrltest

import "testing"

// fataller is the slice of *testing.T that knownDefect needs. Its glue is
// tested against a recorder that implements this interface instead of
// *testing.T, because Fatalf calls runtime.Goexit and a fake that had to
// survive that call is the trap this package already refused.
type fataller interface {
	Helper()
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
}

// KnownDefect pins a live operator defect. check must return a non-nil error
// while the defect is present, and nil once it is gone.
//
// While the defect is present the test passes and logs. When check returns nil
// the defect has been fixed and the test FAILS, telling the author to delete
// the pin and assert the correct behaviour instead. A pin is therefore a claim
// with an expiry, not a suppression, where the expiry is either a fix or a
// design ruling; see the header comment at the top of this file for the second
// case, which is the one that can stay live for a long time legitimately.
//
// check must return an error only from the assertion it is pinning, never from
// its own setup. check() != nil is read as confirmation the defect is still
// live, so a check that fails for an unrelated reason (a typo, an envtest
// hiccup, a field that got renamed out from under it) keeps the pin green long
// after the real defect is fixed. A setup failure should fail the test
// directly, before KnownDefect is ever called.
func KnownDefect(t *testing.T, ref string, check func() error) {
	t.Helper()
	knownDefect(t, ref, check)
}

// knownDefect is KnownDefect's body, written against fataller rather than
// *testing.T so the Fatalf/Logf glue itself, not just the decision behind it,
// can be exercised by a recorder in place of a real test failure.
//
// ref is validated before check runs, not after: an untraceable pin is the
// cheapest possible failure and should not wait on a check body that may poll
// against envtest for seconds and carry its own side effects.
func knownDefect(f fataller, ref string, check func() error) {
	f.Helper()

	var err error
	if ref != "" {
		err = check()
	}

	log, fatal := knownDefectOutcome(ref, err)
	if fatal != "" {
		f.Fatalf("%s", fatal)
		return
	}
	f.Logf("%s", log)
}

// knownDefectOutcome is knownDefect's decision, extracted so it can be table
// tested without faking *testing.T: a body that calls t.Fatalf cannot be
// inverted, because Fatalf calls runtime.Goexit.
func knownDefectOutcome(ref string, err error) (log string, fatal string) {
	if ref == "" {
		return "", "KnownDefect: ref must not be empty; a pin nobody can trace is worse than no pin"
	}
	if err != nil {
		return "known defect " + ref + " still present: " + err.Error(), ""
	}
	return "", "known defect " + ref + " appears fixed; replace this pin with a positive assertion of the correct behaviour"
}
