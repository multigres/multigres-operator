package ctrltest

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// fatallerRecorder implements fataller by recording calls instead of acting
// on them, so knownDefect's Fatalf/Logf glue can be checked without needing to
// survive Fatalf's call to runtime.Goexit.
type fatallerRecorder struct {
	fatalfCalls []string
	logfCalls   []string
}

func (r *fatallerRecorder) Helper() {}

func (r *fatallerRecorder) Fatalf(format string, args ...any) {
	r.fatalfCalls = append(r.fatalfCalls, fmt.Sprintf(format, args...))
}

func (r *fatallerRecorder) Logf(format string, args ...any) {
	r.logfCalls = append(r.logfCalls, fmt.Sprintf(format, args...))
}

// TestKnownDefectGlue covers the four lines knownDefectOutcome's own table
// test cannot reach: the Fatalf/Logf dispatch in knownDefect. Without this,
// inverting the fatal guard, deleting it, or swapping Fatalf for Logf all left
// the suite green, because the pure decision function was the only thing
// under test.
func TestKnownDefectGlue(t *testing.T) {
	cases := []struct {
		name          string
		ref           string
		err           error
		wantFatal     []string
		wantLog       []string
		wantCheckCall bool
	}{
		{
			name:          "error present logs and does not fatal",
			ref:           "MGO-123",
			err:           errors.New("status never reaches Ready"),
			wantLog:       []string{"MGO-123", "status never reaches Ready"},
			wantCheckCall: true,
		},
		{
			name: "error nil fatals as fixed",
			ref:  "MGO-123",
			err:  nil,
			wantFatal: []string{
				"MGO-123",
				"appears fixed",
				"replace this pin with a positive assertion",
			},
			wantCheckCall: true,
		},
		{
			name:          "empty ref fatals without running check",
			ref:           "",
			err:           errors.New("would surface if check ran"),
			wantFatal:     []string{"ref must not be empty"},
			wantCheckCall: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &fatallerRecorder{}
			checkCalled := false
			check := func() error {
				checkCalled = true
				return c.err
			}

			knownDefect(rec, c.ref, check)

			if checkCalled != c.wantCheckCall {
				t.Errorf("check called = %v, want %v", checkCalled, c.wantCheckCall)
			}

			if len(c.wantFatal) == 0 {
				if len(rec.fatalfCalls) != 0 {
					t.Fatalf("Fatalf called with %v, want no Fatalf call", rec.fatalfCalls)
				}
				if len(rec.logfCalls) != 1 {
					t.Fatalf("Logf called %d times, want exactly 1", len(rec.logfCalls))
				}
				for _, want := range c.wantLog {
					if !strings.Contains(rec.logfCalls[0], want) {
						t.Errorf("Logf message %q does not contain %q", rec.logfCalls[0], want)
					}
				}
				return
			}

			if len(rec.logfCalls) != 0 {
				t.Fatalf("Logf called with %v, want no Logf call", rec.logfCalls)
			}
			if len(rec.fatalfCalls) != 1 {
				t.Fatalf("Fatalf called %d times, want exactly 1", len(rec.fatalfCalls))
			}
			for _, want := range c.wantFatal {
				if !strings.Contains(rec.fatalfCalls[0], want) {
					t.Errorf("Fatalf message %q does not contain %q", rec.fatalfCalls[0], want)
				}
			}
		})
	}
}

func TestKnownDefectOutcome(t *testing.T) {
	cases := []struct {
		name      string
		ref       string
		err       error
		wantLog   []string
		wantFatal []string
	}{
		{
			name:    "error present logs and does not fail",
			ref:     "MGO-123",
			err:     errors.New("status never reaches Ready"),
			wantLog: []string{"MGO-123", "status never reaches Ready"},
		},
		{
			name: "error nil fails as fixed",
			ref:  "MGO-123",
			err:  nil,
			wantFatal: []string{
				"MGO-123",
				"appears fixed",
				"replace this pin with a positive assertion",
			},
		},
		{
			name:      "empty ref fails regardless of check",
			ref:       "",
			err:       errors.New("status never reaches Ready"),
			wantFatal: []string{"ref must not be empty"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			log, fatal := knownDefectOutcome(c.ref, c.err)

			if len(c.wantFatal) == 0 {
				if fatal != "" {
					t.Fatalf("fatal = %q, want empty", fatal)
				}
				for _, want := range c.wantLog {
					if !strings.Contains(log, want) {
						t.Errorf("log = %q, want it to contain %q", log, want)
					}
				}
				return
			}

			if fatal == "" {
				t.Fatalf("fatal is empty, want non-empty containing %v", c.wantFatal)
			}
			for _, want := range c.wantFatal {
				if !strings.Contains(fatal, want) {
					t.Errorf("fatal = %q, want it to contain %q", fatal, want)
				}
			}
			// A stray log alongside a fatal is invisible today, since Fatalf
			// exits before KnownDefect's own Logf runs, but this pins the
			// decision function's own contract in case that glue changes.
			if log != "" {
				t.Errorf("log = %q, want empty when fatal is set", log)
			}
		})
	}
}

// TestKnownDefectOutcomeEmptyRefFailsEvenWithoutAnError pins the guard by name:
// deleting it falls through to the "appears fixed" branch, which is also
// non-empty, so asserting only fatal != "" would not catch the guard's
// removal.
func TestKnownDefectOutcomeEmptyRefFailsEvenWithoutAnError(t *testing.T) {
	_, fatal := knownDefectOutcome("", nil)
	if !strings.Contains(fatal, "ref must not be empty") {
		t.Fatalf("fatal = %q, want it to contain %q", fatal, "ref must not be empty")
	}
}

// TestKnownDefectDelegates covers the exported wrapper's own body, which the
// fataller indirection narrowed to one statement but did not reach. Emptying
// that statement turns every pin in the repository into a silent no-op, and
// nothing else in this file notices: the glue tests all drive knownDefect
// directly.
//
// A pin whose defect is live passes, so this is the one case that can be
// called with the real *testing.T, and whether check ran is the one effect a
// passing test can observe from outside.
func TestKnownDefectDelegates(t *testing.T) {
	called := false
	KnownDefect(t, "MGO-DELEGATION", func() error {
		called = true
		return errors.New("defect still present")
	})
	if !called {
		t.Fatal(
			"KnownDefect never ran check, so it is not delegating to knownDefect and every pin is a silent no-op",
		)
	}
}
