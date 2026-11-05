package suite

import (
	"context"
	"errors"
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

// defaultSimCell is the cell every fixture uses, and secondSimCell the one a
// test appends to grow a cluster. The topo store needs its cells declared up
// front, so a test using any other cell name needs simCells widened.
const (
	defaultSimCell = "zone-a"
	secondSimCell  = "zone-b"
)

var simCells = []string{defaultSimCell, secondSimCell}

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
	store, factory := memorytopo.NewServerAndFactory(r.ctx, simCells...)
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

// closeAll closes every store the registry created and forgets them.
func (r *topoRegistry) closeAll() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for k, s := range r.stores {
		errs = append(errs, s.Close())
		delete(r.stores, k)
		delete(r.facts, k)
	}
	return errors.Join(errs...)
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

	mu         sync.Mutex
	registered map[string]bool
	held       map[string]bool
	models     map[string]poolerModel
	// unreachable holds the component IDs whose Status RPC fails, read by
	// faultingRPC on the operator's side of the fake.
	unreachable map[string]bool
	// answering maps a component ID in a modelled namespace to the pod
	// incarnation the sim has registered for it.
	answering map[string]podIncarnation
	// terms tracks each modelled cohort's leader and coordinator term.
	terms map[string]simTerm
}

type simTerm struct {
	leader string
	term   int64
}

type podIncarnation struct{ ns, uid string }

// poolerModel replaces the sim's defaults for one namespace.
//
// A modelled namespace also answers Status only for the pod incarnation the
// sim has registered. Without that, a replacement pod would answer with its
// predecessor's healthy response the moment it was created, since both share
// a component ID, and a test could not tell an operator that waits for the
// replacement to serve from one that does not.
//
// testkit's DataPlaneSim models what the kubelet reports about a Pod
// (readiness, crashloops, scheduling) but nothing a pod's own process reports
// over RPC, so a Pod that is not Ready while its pooler still serves, or a
// pooler that is down while its Pod is Running, has no testkit primitive.
// This is the local stand-in until testkit grows a data-plane RPC fault model.
type poolerModel struct {
	// durability is the policy the cohort is committed under. The default,
	// AtLeastN(1), lets any single pooler satisfy durability alone, so
	// posture.CheckDisruption's revocation check refuses every removal. A
	// test that needs a pod replaced uses three pods and AtLeastN(2).
	durability *cm.DurabilityPolicy
	// fault decides, on every tick, how a pod's pooler deviates from
	// healthy. nil means healthy.
	fault func(*corev1.Pod) *poolerFault
}

// poolerFault is how one pooler's answer deviates from the sim's healthy one.
type poolerFault struct {
	// unreachable fails the Status RPC, as for a multipooler that is down.
	unreachable bool
	// notServing answers with PostgreSQL not accepting connections, as for
	// a pooler whose Postgres is crashlooping.
	notServing bool
	// action reports a long-running Postgres operation in flight.
	action md.PostgresAction
	// neverRegisters keeps the pooler out of topology, as for a multipooler
	// that cannot start on its spec.
	neverRegisters bool
}

// Model installs m for ns until the returned function is called.
func (p *poolerSim) Model(ns string, m poolerModel) func() {
	p.mu.Lock()
	if p.models == nil {
		p.models = map[string]poolerModel{}
	}
	p.models[ns] = m
	p.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			delete(p.models, ns)
			p.mu.Unlock()
		})
	}
}

// SetFault replaces only the fault function of ns's installed model.
func (p *poolerSim) SetFault(ns string, fault func(*corev1.Pod) *poolerFault) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.models[ns]
	m.fault = fault
	if p.models == nil {
		p.models = map[string]poolerModel{}
	}
	p.models[ns] = m
}

// termFor returns the coordinator term for a topology's cohort in ns,
// starting a new one whenever its leader changes.
func (p *poolerSim) termFor(k topoKey, ns, leader string) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terms == nil {
		p.terms = map[string]simTerm{}
	}
	key := registrationPrefix(k) + ns
	t, ok := p.terms[key]
	switch {
	case !ok:
		t = simTerm{leader: leader, term: 2}
	case t.leader != leader:
		t = simTerm{leader: leader, term: t.term + 1}
	}
	p.terms[key] = t
	return t.term
}

func (p *poolerSim) isUnreachable(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.unreachable[id]
}

// notYetAnswering reports whether id belongs to a modelled namespace whose
// current pod is not the incarnation the sim registered: deleted,
// terminating, or recreated and not registered yet.
func (p *poolerSim) notYetAnswering(ctx context.Context, id *cm.ID) bool {
	p.mu.Lock()
	inc, ok := p.answering[string(topoclient.ComponentIDString(id))]
	p.mu.Unlock()
	if !ok {
		return false
	}
	pod := &corev1.Pod{}
	if err := p.c.Get(
		ctx,
		client.ObjectKey{Namespace: inc.ns, Name: id.GetName()},
		pod,
	); err != nil {
		return true
	}
	return string(pod.UID) != inc.uid || !pod.DeletionTimestamp.IsZero()
}

func (p *poolerSim) setAnswering(id string, inc podIncarnation) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.answering == nil {
		p.answering = map[string]podIncarnation{}
	}
	p.answering[id] = inc
}

func (p *poolerSim) setUnreachable(id string, unreachable bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.unreachable == nil {
		p.unreachable = map[string]bool{}
	}
	if unreachable {
		p.unreachable[id] = true
	} else {
		delete(p.unreachable, id)
	}
}

// faultingRPC is the operator's view of the pooler fake: it fails the Status
// RPC of any pooler the sim marks unreachable. rpcclient.FakeClient's own
// Errors map has no locked setter, so writing it while the operator reads it
// would race.
type faultingRPC struct {
	*rpcclient.FakeClient
	sim *poolerSim
}

var errPoolerUnreachable = errors.New("pooler unreachable (sim fault)")

func (f faultingRPC) Status(
	ctx context.Context,
	pooler *cm.Multipooler,
	req *md.StatusRequest,
) (*md.StatusResponse, error) {
	if f.sim.isUnreachable(string(topoclient.ComponentIDString(pooler.GetId()))) ||
		f.sim.notYetAnswering(ctx, pooler.GetId()) {
		return nil, errPoolerUnreachable
	}
	return f.FakeClient.Status(ctx, pooler, req)
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
	// One group per shard on one topology. Two TableGroups in a namespace
	// share a topology but are separate shards, each with its own primary;
	// grouping by topology alone elects one leader across both, so every pod
	// of the other shard never sees a primary and its Shard requeues forever
	// on "No primary in podRoles". reconcilePodRoles scopes roles by the same
	// four labels, so this key matches production.
	type group struct {
		ns                                   string
		topo                                 topoKey
		cluster, database, tableGroup, shard string
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
		g := group{
			ns:         pod.Namespace,
			topo:       k,
			cluster:    pod.Labels[metadata.LabelMultigresCluster],
			database:   pod.Labels[metadata.LabelMultigresDatabase],
			tableGroup: pod.Labels[metadata.LabelMultigresTableGroup],
			shard:      pod.Labels[metadata.LabelMultigresShard],
		}
		groups[g] = append(groups[g], pod)
	}
	for g, pods := range groups {
		p.tickShard(ctx, g.ns, g.topo, pods)
	}
}

// tickShard elects one primary among the pods of one shard and registers
// each with that shard's topology.
func (p *poolerSim) tickShard(
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
	p.mu.Lock()
	model, modelled := p.models[ns]
	p.mu.Unlock()
	durability := model.durability
	if durability == nil {
		durability = topoclient.AtLeastN(1)
	}
	faults := make([]*poolerFault, len(pods))
	cohort := ids
	followers := ids[1:]
	term := int64(2)
	if modelled {
		// A modelled namespace is closer to multiorch: the cohort is the
		// leader plus the poolers registered under their current pod,
		// connected followers only those that could be streaming (answering,
		// with Postgres up), and a new leader starts a new term. The leader
		// stays the lowest-named pod even while its replacement registers,
		// because the sim writes a pooler's routing role only once, at
		// registration.
		cohort, followers = nil, []*cm.ID{}
		for i, pod := range pods {
			if model.fault != nil {
				faults[i] = model.fault(pod)
			}
			p.mu.Lock()
			registered := p.registered[registrationKey(k, ns, string(pod.UID))]
			p.mu.Unlock()
			if !registered && i > 0 {
				continue
			}
			cohort = append(cohort, ids[i])
			if i > 0 && (faults[i] == nil || (!faults[i].unreachable && !faults[i].notServing)) {
				followers = append(followers, ids[i])
			}
		}
		term = p.termFor(k, ns, leader.Name)
	}
	rule := &cm.ShardRule{
		RuleNumber:       &cm.RuleNumber{CoordinatorTerm: term},
		LeaderId:         leader,
		CohortMembers:    cohort,
		DurabilityPolicy: durability,
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
				ConnectedFollowers: followers,
			}
		}
		fault := faults[i]
		if fault != nil {
			resp.Status.PostgresReady = !fault.notServing
			resp.Status.PostgresAction = fault.action
			if fault.notServing && resp.Status.PrimaryStatus != nil {
				resp.Status.PrimaryStatus.Ready = false
			}
		}
		p.setUnreachable(
			string(topoclient.ComponentIDString(id)),
			fault != nil && fault.unreachable,
		)
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
		if already {
			if modelled {
				p.setAnswering(string(topoclient.ComponentIDString(id)),
					podIncarnation{ns: ns, uid: string(pod.UID)})
			}
			continue
		}
		if heldBack || (fault != nil && fault.neverRegisters) {
			continue
		}
		pooler := &cm.Multipooler{
			Id:       id,
			Hostname: pod.Name,
			ShardRef: &cm.ShardRef{
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
