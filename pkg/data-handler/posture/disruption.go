package posture

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"google.golang.org/protobuf/proto"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
)

// ErrInsufficientRemainder is the refusal when the committed cohort without
// the target is too small to stay durable or to elect a leader, the one
// refusal more poolers can resolve.
var ErrInsufficientRemainder = errors.New("remaining cohort is insufficient")

// ErrTopologyUnavailable is the observation failing because the shard's
// topology could not be read; its specific cause varies between attempts.
var ErrTopologyUnavailable = errors.New("topology unavailable")

type topologyUnavailable struct{ error }

func (e topologyUnavailable) Is(target error) bool { return target == ErrTopologyUnavailable }

func (e topologyUnavailable) Unwrap() error { return e.error }

// insufficientRemainder is ErrInsufficientRemainder with its specific cause.
type insufficientRemainder struct{ error }

func (e insufficientRemainder) Is(target error) bool { return target == ErrInsufficientRemainder }

func (e insufficientRemainder) Unwrap() error { return e.error }

// DisruptionTarget describes the pod being removed.
type DisruptionTarget struct {
	Name string
	// Unscheduled must come from a fresh Kubernetes observation. Only a pod
	// that has never scheduled may be removed without a topology registration,
	// unless MayBeUnregistered is set.
	Unscheduled bool
	// MayBeUnregistered lets a scheduled pod with no topology registration be
	// removed, for a caller replacing a pod whose spec keeps its pooler from
	// ever starting. It weakens nothing about the cohort: with no target
	// identity to exclude, every committed member, including any member under
	// this pod's own identity, must still be observed and settled.
	MayBeUnregistered bool
}

// DisruptionObservation is one fresh look at every pooler a disruption
// decision may depend on. Collecting it is separate from deciding, so a pooler
// that cannot be observed blocks only the decisions that need it: a committed
// cohort member that does not answer still blocks every removal, while a
// broken pod outside the cohort does not hold back the repair of another.
type DisruptionObservation struct {
	// registered maps a pod name to the identity its topology record carries,
	// whether or not the pooler answered.
	registered map[string]*clustermetadatapb.ID
	// observed maps a pooler identity to the Status it reported.
	observed map[string]*multipoolermanagerdatapb.StatusResponse
	// unobserved maps a registered pod name to why it reported nothing usable.
	unobserved map[string]error
	// stalePrimaries holds pods still running as primary on a rule older
	// than the topology primary's.
	stalePrimaries map[string]bool
	leader         *clustermetadatapb.ID
	rule           *clustermetadatapb.ShardRule
}

// CheckDisruption observes the data plane afresh before a planned removal.
// Kubernetes readiness and cached topology roles alone cannot establish that
// a previous cohort change or leader election has finished. Missing evidence
// blocks removal; this function never changes Multigres consensus state.
// Status is best-effort: this preflight is not an atomic availability reservation.
func CheckDisruption(
	ctx context.Context,
	store topoclient.Store,
	rpc rpcclient.MultipoolerClient,
	shard *multigresv1alpha1.Shard,
	podNames []string,
	targetPod DisruptionTarget,
) error {
	names := append(append([]string{}, podNames...), targetPod.Name)
	observation, err := ObserveDisruption(ctx, store, rpc, shard, names)
	if err != nil {
		return err
	}
	return observation.Check(shard, targetPod)
}

// observeConcurrency bounds the Status RPCs ObserveDisruption has in flight.
// They run concurrently so a few unreachable poolers, each costing a full
// statusRPCTimeout, cannot exhaust a caller's deadline on their own.
const observeConcurrency = 8

// ObserveDisruption asks every registered pooler matching podNames for its
// Status. A pooler that fails the RPC or reports no consensus identity is
// recorded as unobserved rather than failing the observation; Check decides
// whether that absence matters. Evidence that contradicts itself (an identity
// other than the registered one, a duplicate identity, or primaries whose
// claims cannot be ordered) fails the observation outright, because no
// decision is safe on top of it.
func ObserveDisruption(
	ctx context.Context,
	store topoclient.Store,
	rpc rpcclient.MultipoolerClient,
	shard *multigresv1alpha1.Shard,
	podNames []string,
) (*DisruptionObservation, error) {
	o := &DisruptionObservation{
		registered:     map[string]*clustermetadatapb.ID{},
		observed:       map[string]*multipoolermanagerdatapb.StatusResponse{},
		unobserved:     map[string]error{},
		stalePrimaries: map[string]bool{},
	}
	type probe struct {
		name   string
		pooler *topoclient.MultipoolerInfo
		resp   *multipoolermanagerdatapb.StatusResponse
		err    error
	}
	var probes []*probe
	for _, cell := range topo.CollectCells(shard) {
		poolers, err := store.GetMultipoolersByCell(ctx, cell, topo.ShardFilter(shard))
		if err != nil {
			return nil, topologyUnavailable{fmt.Errorf("observe cell %s: %w", cell, err)}
		}
		for _, pooler := range poolers {
			name := matchPod(pooler, podNames)
			if name == "" || pooler.Id == nil {
				continue
			}
			o.registered[name] = pooler.Id
			probes = append(probes, &probe{name: name, pooler: pooler})
		}
	}

	// A probe still waiting for a slot when the caller's deadline passes is
	// recorded unobserved like any other failed RPC, so the poolers that did
	// answer still decide, and a committed member that did not still blocks.
	var wg sync.WaitGroup
	slots := make(chan struct{}, observeConcurrency)
	for _, p := range probes {
		wg.Go(func() {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				p.err = ctx.Err()
				return
			}
			defer func() { <-slots }()
			rpcCtx, cancel := context.WithTimeout(ctx, statusRPCTimeout)
			defer cancel()
			p.resp, p.err = rpc.Status(
				rpcCtx,
				p.pooler.Multipooler,
				&multipoolermanagerdatapb.StatusRequest{},
			)
		})
	}
	wg.Wait()

	var topoPrimary, others []*probe
	for _, p := range probes {
		if p.err != nil {
			o.unobserved[p.name] = p.err
			continue
		}
		if !proto.Equal(p.resp.GetConsensusStatus().GetId(), p.pooler.Id) {
			if p.resp.GetConsensusStatus().GetId() == nil {
				o.unobserved[p.name] = fmt.Errorf("no consensus status")
				continue
			}
			return nil, fmt.Errorf("pooler %s has no matching consensus identity", p.name)
		}
		key := topoclient.ClusterIDString(p.pooler.Id)
		if _, duplicate := o.observed[key]; duplicate {
			return nil, fmt.Errorf("duplicate pooler identity %s", key)
		}
		o.observed[key] = p.resp
		if p.resp.GetStatus().GetPostgresStatus() !=
			multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY {
			continue
		}
		if topo.IsPrimaryPooler(p.pooler.Multipooler) {
			topoPrimary = append(topoPrimary, p)
		} else {
			others = append(others, p)
		}
	}
	if len(topoPrimary)+len(others) == 0 {
		return o, nil
	}
	if len(topoPrimary) != 1 {
		return nil, fmt.Errorf("primary observations disagree")
	}
	leader := topoPrimary[0]
	o.leader = leader.pooler.Id
	o.rule = leader.resp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetDecision()
	// A pooler that still runs as primary but that topology no longer routes
	// to is a primary that has not yet been demoted, such as one left on a
	// spec that keeps it from rejoining. It is ordered behind the topology
	// primary only when its own committed rule is strictly older; otherwise
	// either could be the real leader and nothing is decided.
	leaderPosition := &clustermetadatapb.RulePosition{Decision: o.rule}
	for _, p := range others {
		position := p.resp.GetConsensusStatus().GetCurrentPosition().GetPosition()
		if position.GetProposal() != nil ||
			consensus.CompareRulePosition(position, leaderPosition) >= 0 {
			return nil, fmt.Errorf("primary observations disagree")
		}
		o.stalePrimaries[p.name] = true
	}
	return o, nil
}

// Serving reports whether podName's pooler answered as an eligible member of
// its committed cohort with PostgreSQL accepting connections: the same
// judgement the pooler readiness gate projects, made from this observation
// rather than from a Pod condition that may be stale or wrong.
func (o *DisruptionObservation) Serving(podName string) bool {
	id := o.registered[podName]
	if id == nil {
		return false
	}
	_, readiness := poolerReadiness(o.observed[topoclient.ClusterIDString(id)], id)
	return readiness.Ready
}

// PostgresAction returns the long-running Postgres operation podName's pooler
// reported, and false when the pooler was not observed.
func (o *DisruptionObservation) PostgresAction(
	podName string,
) (multipoolermanagerdatapb.PostgresAction, bool) {
	id := o.registered[podName]
	if id == nil {
		return multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_UNSPECIFIED, false
	}
	resp := o.observed[topoclient.ClusterIDString(id)]
	if resp == nil {
		return multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_UNSPECIFIED, false
	}
	return resp.GetStatus().GetPostgresAction(), true
}

// Unobserved returns, for each registered pod whose pooler reported nothing
// usable, why. Refusals name these pods without the error text, which varies
// between attempts, so callers that surface a refusal in status can log the
// detail instead.
func (o *DisruptionObservation) Unobserved() map[string]error {
	return maps.Clone(o.unobserved)
}

// unobservedMember names a committed member that did not answer, so a
// blocked removal says which pooler it is waiting for.
func (o *DisruptionObservation) unobservedMember(member *clustermetadatapb.ID) string {
	for name, id := range o.registered {
		if proto.Equal(id, member) {
			if o.unobserved[name] != nil {
				return fmt.Sprintf(" (pod %s did not answer)", name)
			}
			return ""
		}
	}
	return " (not registered in topology)"
}

// Check decides whether removing targetPod is safe on this observation.
func (o *DisruptionObservation) Check(
	shard *multigresv1alpha1.Shard,
	targetPod DisruptionTarget,
) error {
	targetName := targetPod.Name
	target := o.registered[targetName]
	leader, rule, observed := o.leader, o.rule, o.observed
	if (target == nil && !targetPod.Unscheduled && !targetPod.MayBeUnregistered) || leader == nil ||
		rule.GetRuleNumber() == nil ||
		!proto.Equal(rule.GetLeaderId(), leader) {
		return fmt.Errorf("awaiting a registered target and a committed primary%s",
			o.unobservedSummary())
	}
	// A primary left behind on an older rule is split-brain evidence. The
	// only removal it permits is its own, which fences it.
	for _, name := range slices.Sorted(maps.Keys(o.stalePrimaries)) {
		if name != targetName {
			return fmt.Errorf("pooler %s still runs as primary on an older rule", name)
		}
	}
	if proto.Equal(target, leader) && shard.Status.PodRoles[targetName] != "PRIMARY" {
		return fmt.Errorf("target became primary; refresh removal ordering")
	}
	if targetStatus := observed[topoclient.ClusterIDString(target)]; targetStatus != nil {
		position := targetStatus.GetConsensusStatus().GetCurrentPosition().GetPosition()
		comparison := consensus.CompareRulePosition(
			position,
			&clustermetadatapb.RulePosition{Decision: rule},
		)
		// An unhealthy extra may be behind the surviving quorum. A proposal,
		// newer decision, or conflicting decision at the same position is not
		// evidence of completed recovery and must not be ignored.
		conflicting := comparison == 0 && !proto.Equal(position.GetDecision(), rule)
		if position.GetProposal() != nil || comparison > 0 || conflicting {
			return fmt.Errorf("target has an unsettled consensus rule")
		}
	}
	primary := observed[topoclient.ClusterIDString(leader)]
	if _, readiness := poolerReadiness(primary, leader); !readiness.Ready {
		return fmt.Errorf("primary is not an eligible committed cohort member")
	}
	leadership := primary.GetAvailabilityStatus().GetLeadershipStatus()
	// The pinned server omits leadership status unless it has recorded a
	// resignation. Match Multiorch's LeaderNeedsReplacement term qualification:
	// an old resignation is not evidence that the current primary is resigning.
	// Pending primary proposals are rejected by the cohort checks below.
	resigning := leadership.GetSignal() == clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION &&
		leadership.GetLeaderTerm() != 0 &&
		leadership.GetLeaderTerm() == rule.GetRuleNumber().GetCoordinatorTerm()
	if resigning || !primary.GetStatus().GetPrimaryStatus().GetReady() {
		return fmt.Errorf("committed primary is not actively serving")
	}
	var remaining []*clustermetadatapb.ID
	// An unregistered, unscheduled target has no identity to exclude. Require
	// observations for every committed member in that case: a stale membership
	// for this pod (or any other missing member) must still block cleanup.
	for _, member := range rule.GetCohortMembers() {
		if proto.Equal(member, target) && !proto.Equal(member, leader) {
			continue
		}
		resp := observed[topoclient.ClusterIDString(member)]
		_, ready := poolerReadiness(resp, member)
		cs := resp.GetConsensusStatus()
		position := cs.GetCurrentPosition().GetPosition()
		settled := proto.Equal(position.GetDecision(), rule) && position.GetProposal() == nil
		eligible := !consensus.IsSelfRevoked(cs) && cs.GetRecruitBlockedUntil() == nil
		if !ready.Ready || !settled || !eligible {
			return fmt.Errorf(
				"cohort member %s has not recovered on the committed rule%s",
				topoclient.ClusterIDString(member),
				o.unobservedMember(member),
			)
		}
		if !proto.Equal(member, target) {
			if !proto.Equal(member, leader) &&
				resp.GetStatus().
					GetPostgresStatus() !=
					multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY {
				return fmt.Errorf(
					"surviving follower %s is not a standby",
					topoclient.ClusterIDString(member),
				)
			}
			remaining = append(remaining, member)
		}
	}
	policy, err := consensus.NewPolicyFromProto(rule.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("invalid committed durability policy: %w", err)
	}
	if err := policy.SatisfiedBy(remaining); err != nil {
		return insufficientRemainder{
			fmt.Errorf("remaining cohort cannot satisfy durability: %w", err),
		}
	}
	if err := consensus.CheckSufficientRecruitment(
		policy,
		rule.GetCohortMembers(),
		remaining,
	); err != nil {
		return insufficientRemainder{
			fmt.Errorf("remaining cohort cannot recruit a leader: %w", err),
		}
	}
	// Require the current primary to report connections to the remaining
	// followers. A locally accepting standby can still be disconnected after a
	// failover, despite having replicated the same rule earlier. ConnectedFollowers
	// reports pg_stat_replication membership, not WAL catch-up or streaming state.
	connected := map[string]bool{topoclient.ClusterIDString(leader): true}
	for _, follower := range primary.GetStatus().GetPrimaryStatus().GetConnectedFollowers() {
		connected[topoclient.ClusterIDString(follower)] = true
	}
	for _, member := range remaining {
		if !connected[topoclient.ClusterIDString(member)] {
			return fmt.Errorf(
				"cohort member %s is not connected to the primary",
				topoclient.ClusterIDString(member),
			)
		}
	}
	return nil
}

// unobservedSummary lists the poolers that did not answer, for a refusal that
// may have been caused by one of them being the primary.
func (o *DisruptionObservation) unobservedSummary() string {
	if len(o.unobserved) == 0 {
		return ""
	}
	return "; did not answer: " + strings.Join(slices.Sorted(maps.Keys(o.unobserved)), ", ")
}
