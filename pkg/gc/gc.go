// Package gc provides the generic garbage-collector framework used by the
// multigres-gc binary.
//
// Each resource type that needs cleaning (PVCs today, more later) implements
// the Cleankeeper interface in its own sub-package. The binary discovers
// cleankeepers at startup, invokes Clean on each, and aggregates Results.
package gc

import (
	"context"
	"time"
)

// Options is the common configuration passed to every Cleankeeper.
type Options struct {
	// Retention is the minimum age (now - orphan-since date) before a resource is deleted.
	Retention time.Duration

	// Namespace restricts the clean to a single namespace. Empty means all.
	Namespace string
	DryRun    bool

	// Now allows tests to inject a fixed clock. Defaults to time.Now.
	Now func() time.Time
}

// WithDefaults returns a copy of o with required defaults applied.
func (o Options) WithDefaults() Options {
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Results summarises a single Clean over one resource kind.
type Results struct {
	Kind    string
	Scanned int
	Deleted int
	// WouldDelete is the number of objects that would have been deleted in dry-run.
	WouldDelete int
	Skipped     int
	// Malformed is the number of objects whose orphan label could not be parsed.
	Malformed int
	Errors    int
}

// Cleankeeper deletes orphaned resources of one kind.
//
// Implementations should be cheap to construct. Clean does all the work.
// A non-nil error from Clean aborts that resource kind only; main keeps
// running other Cleankeepers.
type Cleankeeper interface {
	// Kind returns the short identifier for the resource type (e.g. "pvc").
	Kind() string

	// Clean performs one pass and returns its results.
	Clean(ctx context.Context) (*Results, error)
}
