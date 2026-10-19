package suite

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// defaultSimCell is the cell every fixture uses. The topo store needs its cells
// declared up front, so a test using a different cell name needs this widened.
const defaultSimCell = "zone-a"

// topoRegistry hands out one in-memory topology store per topology server,
// identified by address and root path, the same pair a multipooler is started
// with.
//
// Per topology rather than one shared store, for two reasons. Namespace-per-test
// is the suite's isolation boundary, and the managed address carries the
// namespace, so a single store would let one test's cluster see another's
// multipoolers. And a cluster can move between topology servers, and a pooler
// left pointing at the old one must be invisible to everything reading the new
// one, or the suite cannot see what that move does to a live shard.
type topoRegistry struct {
	ctx context.Context

	mu     sync.Mutex
	stores map[topoKey]topoclient.Store
	facts  map[topoKey]*memorytopo.Factory
}

type topoKey struct{ address, root string }

func newTopoRegistry(ctx context.Context) *topoRegistry {
	return &topoRegistry{
		ctx:    ctx,
		stores: map[topoKey]topoclient.Store{},
		facts:  map[topoKey]*memorytopo.Factory{},
	}
}

// Store returns the topology's store, creating it on first use.
func (r *topoRegistry) Store(k topoKey) topoclient.Store {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.stores[k]; ok {
		return s
	}
	store, factory := memorytopo.NewServerAndFactory(r.ctx, defaultSimCell)
	r.stores[k] = store
	r.facts[k] = factory
	return store
}

// forgetTopology discards a topology's contents, standing in for a managed etcd
// whose volumes were deleted along with it. The next use starts from empty, and
// every pod on it registers again.
func forgetTopology(k topoKey) error {
	poolers.forget(k)
	return topo.forget(k)
}

func (r *topoRegistry) forget(k topoKey) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.stores[k]
	delete(r.stores, k)
	delete(r.facts, k)
	if !ok {
		return nil
	}
	return s.Close()
}

func (r *topoRegistry) client(k topoKey) (topoclient.Store, error) {
	r.Store(k)
	r.mu.Lock()
	factory := r.facts[k]
	r.mu.Unlock()
	return topoclient.NewWithFactory(
		factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
	), nil
}

// ForShard is ShardReconciler.CreateTopoStore.
func (r *topoRegistry) ForShard(shard *Shard) (topoclient.Store, error) {
	return r.ForClusterRef(shard.Spec.GlobalTopoServer)
}

// ForClusterRef is MultigresClusterReconciler.CreateTopoStore. Left unstubbed,
// this seam dials a real etcd and the cluster never reaches TopologyReady.
func (r *topoRegistry) ForClusterRef(
	ref multigresv1alpha1.GlobalTopoServerRef,
) (topoclient.Store, error) {
	return r.client(topoKey{address: ref.Address, root: ref.RootPath})
}

// podTopo reads the topology a pool pod's multipooler was started against,
// from the same flags the real binary reads it from.
func podTopo(pod *corev1.Pod) (topoKey, bool) {
	var k topoKey
	var haveAddr, haveRoot bool
	for _, c := range pod.Spec.Containers {
		for _, arg := range c.Args {
			if v, ok := strings.CutPrefix(arg, "--topo-global-server-addresses="); ok {
				k.address, haveAddr = v, true
			} else if v, ok := strings.CutPrefix(arg, "--topo-global-root="); ok {
				k.root, haveRoot = v, true
			}
		}
	}
	return k, haveAddr && haveRoot
}

// poolerSim is the data plane: it registers a multipooler per pool pod in the
// topology store that pod's flags name and answers a healthy Status RPC for
// each. Pods sharing a topology form one cohort; a pod left on a topology the
// shard has moved off is registered only there.
//
// Without it the shard controller stalls at PostureConsistent=Unknown
// (AwaitingPoolerRegistration) and never reaches a terminal state.
//
// Deliberately generous: every pod is healthy, always, and the lowest-numbered
// pod is primary. It models no ordering, no failure, and no latency, so a test
// that needs any of those needs a better fake than this one.
type poolerSim struct {
	c        client.Client
	rpc      *rpcclient.FakeClient
	topo     *topoRegistry
	interval time.Duration

	mu            sync.Mutex
	registered    map[string]bool
	held          map[string]bool
	lateAdmission map[string]*admissionDelay
}

// leaderAdmissionTicks is how many sim ticks a new leader spends outside its
// cohort under AdmitLeadersLate: long enough for the operator to observe it.
const leaderAdmissionTicks = 6

// admissionDelay is what AdmitLeadersLate records for one namespace.
type admissionDelay struct {
	ticks map[string]int
	seen  bool
}

// HoldRegistrations stops this fake registering any *new* pooler in ns until
// the returned function is called. Poolers already registered keep answering.
//
// It exists to make a race deterministic instead of sampled. The defect it was
// built for needs a shard to converge having seen fewer poolers than pods,
// which happens on its own only when registration loses a race against the
// last reconcile: about half the time, measured. A test that waits for that by
// chance detects a regression about half the time too, which is what the
// twelve-attempt statistical pin it replaced was paying for.
//
// Holding lets a test construct the precondition on purpose: hold, scale up,
// wait until the shard has demonstrably converged short, then release. One
// attempt, and the regression either survives the release or it does not.
func (p *poolerSim) HoldRegistrations(ns string) func() {
	p.mu.Lock()
	if p.held == nil {
		p.held = map[string]bool{}
	}
	p.held[ns] = true
	p.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			delete(p.held, ns)
			p.mu.Unlock()
		})
	}
}

// AdmitLeadersLate makes this fake commit every new leader in ns outside its
// cohort for leaderAdmissionTicks ticks, then admit it, until the returned
// function is called. This is multiorch's own sequence: a failover keeps the
// outgoing cohort while choosing any eligible leader, and the cohort-mismatch
// analyzer then admits that leader. A topology group with a single pod has no
// cohort to leave the leader out of, so the state needs two pods on one
// topology; the returned function reports whether it was ever committed.
func (p *poolerSim) AdmitLeadersLate(ns string) (release func(), seen func() bool) {
	a := &admissionDelay{ticks: map[string]int{}}
	p.mu.Lock()
	if p.lateAdmission == nil {
		p.lateAdmission = map[string]*admissionDelay{}
	}
	p.lateAdmission[ns] = a
	p.mu.Unlock()

	var once sync.Once
	release = func() {
		once.Do(func() {
			p.mu.Lock()
			delete(p.lateAdmission, ns)
			p.mu.Unlock()
		})
	}
	seen = func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return a.seen
	}
	return release, seen
}

func registrationKey(k topoKey, ns, podUID string) string {
	return registrationPrefix(k) + ns + "/" + podUID
}

func registrationPrefix(k topoKey) string {
	return k.address + "|" + k.root + "|"
}

func (p *poolerSim) forget(k topoKey) {
	prefix := registrationPrefix(k)
	p.mu.Lock()
	defer p.mu.Unlock()
	for key := range p.registered {
		if strings.HasPrefix(key, prefix) {
			delete(p.registered, key)
		}
	}
}

func (p *poolerSim) run(ctx context.Context) {
	p.registered = map[string]bool{}
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

func (p *poolerSim) tick(ctx context.Context) {
	pods := &corev1.PodList{}
	if err := p.c.List(ctx, pods,
		client.MatchingLabels{metadata.LabelAppComponent: shardcontroller.PoolComponentName},
	); err != nil {
		return
	}
	type group struct {
		ns   string
		topo topoKey
	}
	groups := map[group][]*corev1.Pod{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		k, ok := podTopo(pod)
		if !ok {
			continue
		}
		g := group{ns: pod.Namespace, topo: k}
		groups[g] = append(groups[g], pod)
	}
	for g, pods := range groups {
		p.tickTopology(ctx, g.ns, g.topo, pods)
	}
}

func (p *poolerSim) tickTopology(
	ctx context.Context,
	ns string,
	k topoKey,
	pods []*corev1.Pod,
) {
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })

	ids := make([]*cm.ID, 0, len(pods))
	for _, pod := range pods {
		ids = append(ids, &cm.ID{
			Cell: pod.Labels[metadata.LabelMultigresCell],
			Name: pod.Name,
		})
	}
	if len(ids) == 0 {
		return
	}
	leader := ids[0]
	cohort := ids
	p.mu.Lock()
	if a := p.lateAdmission[ns]; a != nil && len(ids) > 1 {
		key := registrationPrefix(k) + leader.Name
		if a.ticks[key] < leaderAdmissionTicks {
			a.ticks[key]++
			a.seen = true
			cohort = ids[1:]
		}
	}
	p.mu.Unlock()
	rule := &cm.ShardRule{
		RuleNumber:       &cm.RuleNumber{CoordinatorTerm: 2},
		LeaderId:         leader,
		CohortMembers:    cohort,
		DurabilityPolicy: topoclient.AtLeastN(1),
	}
	store := p.topo.Store(k)

	for i, pod := range pods {
		id := ids[i]
		role := cm.RoutingRole_ROUTING_ROLE_REPLICA
		resp := &md.StatusResponse{
			Status: &md.Status{
				IsInitialized:  true,
				PostgresReady:  true,
				PostgresStatus: md.PostgresStatus_POSTGRES_STATUS_STANDBY,
			},
			AvailabilityStatus: &cm.AvailabilityStatus{
				CohortEligibilityStatus: &cm.CohortEligibilityStatus{
					Signal: cm.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
			ConsensusStatus: &cm.ConsensusStatus{
				Id:              id,
				CurrentPosition: &cm.PoolerPosition{Position: &cm.RulePosition{Decision: rule}},
			},
		}
		if id.Name == leader.Name {
			role = cm.RoutingRole_ROUTING_ROLE_PRIMARY
			resp.Status.PostgresStatus = md.PostgresStatus_POSTGRES_STATUS_PRIMARY
			resp.Status.PrimaryStatus = &md.PrimaryStatus{
				Ready:              true,
				ConnectedFollowers: ids[1:],
			}
		}
		p.rpc.SetStatusResponse(topoclient.ComponentIDString(id), resp)

		// Keyed by topology and pod UID, not name: a rolled pod keeps its name
		// but is a new process, and a real multipooler registers on every
		// start. Keyed by name, a replacement was never registered, so once the
		// operator pruned its predecessor's entry it was never observed again.
		key := registrationKey(k, ns, string(pod.UID))
		p.mu.Lock()
		already := p.registered[key]
		heldBack := p.held[ns]
		p.mu.Unlock()
		// A held namespace still gets its Status RPC answered above, so pods
		// already registered stay healthy and the shard keeps converging. Only
		// the new registration waits, which is the whole point.
		if already || heldBack {
			continue
		}
		pooler := &cm.Multipooler{
			Id:       id,
			Hostname: pod.Name,
			ShardKey: &cm.ShardKey{
				Database:   pod.Labels[metadata.LabelMultigresDatabase],
				TableGroup: pod.Labels[metadata.LabelMultigresTableGroup],
				Shard:      pod.Labels[metadata.LabelMultigresShard],
			},
			RoutingState: &cm.RoutingState{Role: role},
		}
		if err := store.RegisterMultipooler(ctx, pooler, true); err == nil {
			p.mu.Lock()
			p.registered[key] = true
			p.mu.Unlock()
		}
	}
}
