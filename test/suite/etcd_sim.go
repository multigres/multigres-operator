package suite

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/multigres/testkit/etcdsim"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	toposervercontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/toposerver"
)

// simEntry pairs a simulated cluster with the endpoints it answers on, so the
// factory can refuse a TopoServer whose controller-derived endpoints differ.
type simEntry struct {
	cluster   *etcdsim.Cluster
	endpoints []string
}

var (
	etcdSimMu sync.Mutex
	// etcdSims is keyed by namespace/name of the TopoServer. Cases get distinct
	// namespaces, so concurrent cases never share an entry.
	etcdSims = map[string]simEntry{}
)

func etcdSimKey(namespace, name string) string { return namespace + "/" + name }

// EtcdSim returns the simulated etcd cluster behind the named TopoServer in
// this case's namespace, creating it on first use and returning the same one
// afterwards. The entry is removed when the case ends.
//
// The cluster is built before the TopoServer exists, so its endpoints are
// derived the way the controller derives them: from the name, the namespace
// and members as the replica count, with TLS off. The factory re-derives them
// from the live TopoServer at call time and fails loudly on a mismatch
// (different replicas, TLS enabled), rather than letting every call fall into
// the simulator's unknown-endpoint error.
func (c *C) EtcdSim(topoServerName string, members int) *etcdsim.Cluster {
	c.Helper()
	// members becomes an int32 replica count; checking the range here keeps
	// that conversion lossless instead of wrapping a bad value into a
	// plausible-looking endpoint list.
	if members < 1 || members > math.MaxInt32 {
		panic(
			fmt.Sprintf(
				"EtcdSim(%q): members %d out of range [1, %d]",
				topoServerName,
				members,
				math.MaxInt32,
			),
		)
	}
	key := etcdSimKey(c.NS, topoServerName)
	etcdSimMu.Lock()
	defer etcdSimMu.Unlock()
	if e, ok := etcdSims[key]; ok {
		return e.cluster
	}
	endpoints := toposervercontroller.MaintenanceEndpoints(&multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Namespace: c.NS, Name: topoServerName},
		Spec: multigresv1alpha1.TopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{Replicas: ptr.To(int32(members))},
		},
	})
	cluster := etcdsim.NewHealthyAt(endpoints...)
	etcdSims[key] = simEntry{cluster: cluster, endpoints: endpoints}
	c.Cleanup(func() {
		etcdSimMu.Lock()
		defer etcdSimMu.Unlock()
		delete(etcdSims, key)
	})
	return cluster
}

// etcdSimFactory is the TopoServerReconciler.MaintenanceClientFactory for the
// suite. A TopoServer with maintenance enabled and no registered simulator
// fails here instead of dialling a real etcd that does not exist.
func etcdSimFactory(
	_ context.Context,
	ts *multigresv1alpha1.TopoServer,
) (toposervercontroller.EtcdMaintenanceClient, error) {
	etcdSimMu.Lock()
	e, ok := etcdSims[etcdSimKey(ts.Namespace, ts.Name)]
	etcdSimMu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no etcd sim registered for %s/%s", ts.Namespace, ts.Name)
	}
	if got := toposervercontroller.MaintenanceEndpoints(ts); !slices.Equal(got, e.endpoints) {
		return nil, fmt.Errorf(
			"etcd sim for %s/%s answers on %v but the controller dials %v",
			ts.Namespace, ts.Name, e.endpoints, got,
		)
	}
	return e.cluster.Client(), nil
}
