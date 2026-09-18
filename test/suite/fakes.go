package suite

import (
	"context"
	"fmt"
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

// topoRegistry hands out one in-memory topology store per namespace.
//
// Per namespace rather than one shared store, because namespace-per-test is the
// suite's isolation boundary and a single store would let one test's cluster
// see another's multipoolers. Both CreateTopoStore seams resolve through here:
// the shard's carries the object, the cluster's carries only a DNS address, so
// that one recovers the namespace by parsing it.
type topoRegistry struct {
	ctx context.Context

	mu     sync.Mutex
	stores map[string]topoclient.Store
	facts  map[string]*memorytopo.Factory
}

func newTopoRegistry(ctx context.Context) *topoRegistry {
	return &topoRegistry{
		ctx:    ctx,
		stores: map[string]topoclient.Store{},
		facts:  map[string]*memorytopo.Factory{},
	}
}

// Store returns the namespace's store, creating it on first use.
func (r *topoRegistry) Store(ns string) topoclient.Store {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.stores[ns]; ok {
		return s
	}
	store, factory := memorytopo.NewServerAndFactory(r.ctx, defaultSimCell)
	r.stores[ns] = store
	r.facts[ns] = factory
	return store
}

func (r *topoRegistry) client(ns string) (topoclient.Store, error) {
	r.Store(ns)
	r.mu.Lock()
	factory := r.facts[ns]
	r.mu.Unlock()
	return topoclient.NewWithFactory(
		factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
	), nil
}

// ForShard is ShardReconciler.CreateTopoStore.
func (r *topoRegistry) ForShard(shard *multigresv1alpha1.Shard) (topoclient.Store, error) {
	return r.client(shard.Namespace)
}

// ForClusterRef is MultigresClusterReconciler.CreateTopoStore. The ref carries
// no namespace, only the Service address the cluster controller built as
// "<cluster>-global-topo.<namespace>.svc:2379", so the namespace comes back out
// of the address. Left unstubbed, this seam dials a real etcd and the cluster
// never reaches TopologyReady.
func (r *topoRegistry) ForClusterRef(
	ref multigresv1alpha1.GlobalTopoServerRef,
) (topoclient.Store, error) {
	ns, err := namespaceFromTopoAddress(ref.Address)
	if err != nil {
		return nil, err
	}
	return r.client(ns)
}

func namespaceFromTopoAddress(address string) (string, error) {
	parts := strings.Split(address, ".")
	if len(parts) < 2 || parts[1] == "" {
		return "", fmt.Errorf("cannot derive namespace from topo address %q", address)
	}
	return parts[1], nil
}

// poolerSim is the data plane: it registers a multipooler per pool pod in that
// namespace's topology store and answers a healthy Status RPC for each.
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
	byNamespace := map[string][]*corev1.Pod{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		byNamespace[pod.Namespace] = append(byNamespace[pod.Namespace], pod)
	}
	for ns, group := range byNamespace {
		p.tickNamespace(ctx, ns, group)
	}
}

func (p *poolerSim) tickNamespace(ctx context.Context, ns string, pods []*corev1.Pod) {
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
	rule := &cm.ShardRule{
		RuleNumber:       &cm.RuleNumber{CoordinatorTerm: 2},
		LeaderId:         leader,
		CohortMembers:    ids,
		DurabilityPolicy: topoclient.AtLeastN(1),
	}
	store := p.topo.Store(ns)

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

		key := ns + "/" + pod.Name
		p.mu.Lock()
		already := p.registered[key]
		p.mu.Unlock()
		if already {
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
