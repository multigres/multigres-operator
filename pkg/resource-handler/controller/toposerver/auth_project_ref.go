package toposerver

import (
	"context"
	"errors"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// clusterRefusal is a referencing cluster that an auth-managed server grants
// nothing, because it has no project reference or a root outside its project
// prefix. A derived root can sit inside another cluster's prefix. On an active
// server, a cluster whose own topology configuration cannot be authorized is
// refused too, with invalidConfig set.
type clusterRefusal struct {
	cluster       client.ObjectKey
	reason        string
	invalidConfig bool
}

type refusedClustersError struct {
	refused []clusterRefusal
}

func (e *refusedClustersError) Error() string {
	parts := make([]string, 0, len(e.refused))
	for _, refusal := range e.refused {
		parts = append(parts, refusal.cluster.String()+" "+refusal.reason)
	}
	return "refused clusters keep the topology access they already have, and get no new access, " +
		"until they are fixed or deleted: " + cappedList(parts, "; ")
}

func (e *refusedClustersError) invalidConfig() bool {
	return slices.ContainsFunc(e.refused, func(refusal clusterRefusal) bool {
		return refusal.invalidConfig
	})
}

func refusesCluster(refused []clusterRefusal, key client.ObjectKey) bool {
	return slices.ContainsFunc(refused, func(refusal clusterRefusal) bool {
		return refusal.cluster == key
	})
}

// clusterConfigError is a referencing cluster's own topology configuration that
// cannot be authorized, such as custom client credentials or a missing
// template. It is never a failed API read.
type clusterConfigError struct {
	err error
}

func (e *clusterConfigError) Error() string { return e.err.Error() }

func (e *clusterConfigError) Unwrap() error { return e.err }

// configRefusal refuses the cluster for a clusterConfigError on an active
// server, so one tenant does not stop the others from reconciling. Before
// activation, and for any other error, it returns err.
func configRefusal(activated bool, key client.ObjectKey, err error) (clusterRefusal, error) {
	var config *clusterConfigError
	if !activated || !errors.As(err, &config) {
		return clusterRefusal{}, err
	}
	return clusterRefusal{
		cluster:       key,
		reason:        "has a topology configuration that cannot be authorized: " + config.Error(),
		invalidConfig: true,
	}, nil
}

// readFailureRecorder records reads that failed for a reason other than
// NotFound. Resolution errors seen after such a read are retried rather than
// treated as configuration, because the read may succeed next time.
type readFailureRecorder struct {
	client.Reader
	failed bool
}

func (r *readFailureRecorder) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	return r.record(r.Reader.Get(ctx, key, obj, opts...))
}

func (r *readFailureRecorder) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	return r.record(r.Reader.List(ctx, list, opts...))
}

func (r *readFailureRecorder) record(err error) error {
	if err != nil && !apierrors.IsNotFound(err) {
		r.failed = true
	}
	return err
}

func (r *readFailureRecorder) configError(err error) error {
	if r.failed {
		return err
	}
	return &clusterConfigError{err: err}
}
