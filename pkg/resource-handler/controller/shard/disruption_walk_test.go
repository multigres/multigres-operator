package shard

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// walkFixture is a shard of Ready pool Pods in one cell, built by
// BuildPoolPod so every pod is on the desired spec until a test drifts it,
// and observed healthy by RPC under AtLeastN(2). Uncached Pod lists and RPC
// observations of the data plane are counted.
type walkFixture struct {
	r            *ShardReconciler
	base         client.Client
	shard        *multigresv1alpha1.Shard
	rendered     renderedConfig
	names        []string
	rpc          *rpcclient.FakeClient
	responses    []*md.StatusResponse
	podLists     int
	observations int
}

// newWalkFixture builds replicas pods for each pool in pools, in order, and
// makes the pod at index leader, counting across pools, the primary.
func newWalkFixture(t *testing.T, pools []string, replicas int32, leader int) *walkFixture {
	t.Helper()
	return newWalkFixtureIn(t, pools, func(string) string { return "a" }, replicas, leader)
}

// newWalkFixtureIn is newWalkFixture with each pool in the one cell cellOf
// names for it.
func newWalkFixtureIn(
	t *testing.T,
	pools []string,
	cellOf func(pool string) string,
	replicas int32,
	leader int,
) *walkFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = multigresv1alpha1.AddToScheme(scheme)
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shard",
			Namespace: "default",
			UID:       "shard-uid",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "0",
			Pools:          map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
		},
		Status: multigresv1alpha1.ShardStatus{PodRoles: map[string]string{}},
	}
	for _, pool := range pools {
		shard.Spec.Pools[multigresv1alpha1.PoolName(pool)] = multigresv1alpha1.PoolSpec{
			Cells:           []multigresv1alpha1.CellName{multigresv1alpha1.CellName(cellOf(pool))},
			ReplicasPerCell: ptr.To(replicas),
			Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
		}
	}
	f := &walkFixture{
		shard:    shard,
		rendered: renderedConfig{restartHash: "restart", reloadHash: "reload"},
	}
	objects := []client.Object{shard.DeepCopy()}
	// As a pass does before it builds any pod.
	stampPostgresConfigHashes(shard, f.rendered)
	for _, pool := range pools {
		spec := shard.Spec.Pools[multigresv1alpha1.PoolName(pool)]
		for i := range int(replicas) {
			pod, err := BuildPoolPod(shard, pool, cellOf(pool), spec, i, scheme)
			if err != nil {
				t.Fatal(err)
			}
			pod.UID = types.UID("uid-" + pod.Name)
			pod.Spec.NodeName = "node"
			setReady(pod, true)
			f.names = append(f.names, pod.Name)
			objects = append(objects, pod)
		}
	}
	shard.Status.PodRoles[f.names[leader]] = "PRIMARY"
	f.base = fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}, &multigresv1alpha1.Shard{}).
		WithObjects(objects...).
		Build()
	f.r = &ShardReconciler{
		Client:   f.base,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}
	f.r.APIReader = testutil.NewFakeClientWithFailures(f.base, &testutil.FailureConfig{
		OnList: func(list client.ObjectList) error {
			if _, ok := list.(*corev1.PodList); ok {
				f.podLists++
			}
			return nil
		},
	})
	f.rpc, f.responses = observeHealthyDisruption(t, f.r, shard, pools[0], cellOf(pools[0]))
	store := f.r.CreateTopoStore
	f.r.CreateTopoStore = func(s *multigresv1alpha1.Shard) (topoclient.Store, error) {
		f.observations++
		return store(s)
	}
	return f
}

// connect makes the leader report exactly the pods at followers as connected.
func (f *walkFixture) connect(followers ...int) {
	var leader *md.StatusResponse
	for i, response := range f.responses {
		if response.Status.PrimaryStatus != nil {
			leader = proto.Clone(response).(*md.StatusResponse)
			f.responses[i] = leader
		}
	}
	leader.Status.PrimaryStatus.ConnectedFollowers = nil
	for _, i := range followers {
		leader.Status.PrimaryStatus.ConnectedFollowers = append(
			leader.Status.PrimaryStatus.ConnectedFollowers, f.responses[i].ConsensusStatus.Id)
	}
	f.rpc.SetStatusResponse(topoclient.ComponentIDString(leader.ConsensusStatus.Id), leader)
}

func (f *walkFixture) pod(t *testing.T, i int) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: f.shard.Namespace, Name: f.names[i]}
	if err := f.base.Get(t.Context(), key, pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

// drained returns the fixture's pods that exist and carry a drain state.
func (f *walkFixture) drained(t *testing.T) []string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := f.base.List(t.Context(), pods, client.InNamespace(f.shard.Namespace)); err != nil {
		t.Fatal(err)
	}
	var drained []string
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Annotations[metadata.AnnotationDrainState] != "" &&
			slices.Contains(f.names, pod.Name) {
			drained = append(drained, pod.Name)
		}
	}
	slices.Sort(drained)
	return drained
}

func (f *walkFixture) requireDrained(t *testing.T, want ...int) {
	t.Helper()
	var names []string
	for _, i := range want {
		names = append(names, f.names[i])
	}
	slices.Sort(names)
	if got := f.drained(t); !slices.Equal(got, names) {
		t.Fatalf("drained %v, want %v", got, names)
	}
}

func (f *walkFixture) failWrites(config *testutil.FailureConfig) {
	f.r.Client = testutil.NewFakeClientWithFailures(f.base, config)
}

// update changes the pod at index i in the API.
func (f *walkFixture) update(t *testing.T, i int, change func(*corev1.Pod)) {
	t.Helper()
	pod := f.pod(t, i)
	change(pod)
	if err := f.base.Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
}

// drift puts the pods at the given indexes on an old spec.
func (f *walkFixture) drift(t *testing.T, indexes ...int) {
	t.Helper()
	for _, i := range indexes {
		f.update(t, i, func(pod *corev1.Pod) {
			pod.Annotations[metadata.AnnotationSpecHash] = "stale"
		})
	}
}

// notReady turns the pod at index i not Ready.
func (f *walkFixture) notReady(t *testing.T, i int) {
	t.Helper()
	pod := f.pod(t, i)
	setReady(pod, false)
	if err := f.base.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
}

// terminate deletes the pod at index i and holds it terminating.
func (f *walkFixture) terminate(t *testing.T, i int) {
	t.Helper()
	f.update(t, i, func(pod *corev1.Pod) { pod.Finalizers = []string{"test/hold"} })
	if err := f.base.Delete(t.Context(), f.pod(t, i)); err != nil {
		t.Fatal(err)
	}
}

// scale sets the pool's replica count in the spec the pass reads.
func (f *walkFixture) scale(pool string, replicas int32) {
	spec := f.shard.Spec.Pools[multigresv1alpha1.PoolName(pool)]
	spec.ReplicasPerCell = ptr.To(replicas)
	f.shard.Spec.Pools[multigresv1alpha1.PoolName(pool)] = spec
}

// resizePending creates the data PVC of the pool's pod at ordinal, waiting
// for a pod restart to grow its filesystem.
func (f *walkFixture) resizePending(t *testing.T, pool string, ordinal int) {
	t.Helper()
	spec := f.shard.Spec.Pools[multigresv1alpha1.PoolName(pool)]
	pvc, err := BuildPoolDataPVC(f.shard, pool, string(spec.Cells[0]), spec, ordinal,
		false, f.r.Scheme)
	if err != nil {
		t.Fatal(err)
	}
	pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{{
		Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
		Status: corev1.ConditionTrue,
	}}
	if err := f.base.Create(t.Context(), pvc); err != nil {
		t.Fatal(err)
	}
}

// resizeDone clears the pending resize resizePending left on the PVC.
func (f *walkFixture) resizeDone(t *testing.T, pool string, ordinal int) {
	t.Helper()
	spec := f.shard.Spec.Pools[multigresv1alpha1.PoolName(pool)]
	pvc := &corev1.PersistentVolumeClaim{}
	key := client.ObjectKey{
		Namespace: f.shard.Namespace,
		Name:      BuildPoolDataPVCName(f.shard, pool, string(spec.Cells[0]), ordinal),
	}
	if err := f.base.Get(t.Context(), key, pvc); err != nil {
		t.Fatal(err)
	}
	pvc.Status.Conditions = nil
	if err := f.base.Status().Update(t.Context(), pvc); err != nil {
		t.Fatal(err)
	}
}

// drainStates advances the drain state machine one step.
func (f *walkFixture) drainStates(t *testing.T) error {
	t.Helper()
	store, err := f.r.CreateTopoStore(f.shard)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.r.reconcileDrainState(t.Context(), store, f.shard)
	return err
}

var errRefused = errors.New("write refused")

// walkPass is what one reconcile pass left behind.
type walkPass struct {
	n         int
	rollout   *shardRolloutTracker
	poolsErr  error
	walkErr   error
	before    *metav1.Condition
	condition *metav1.Condition
}

// reconcile is one reconcile pass's pool maintenance followed by the
// disruption walk and the RollingUpdate condition. between runs after
// maintenance, where the data plane phase does in a real pass.
func (f *walkFixture) reconcile(
	t *testing.T,
	before *metav1.Condition,
	between func(*testing.T, *walkFixture),
) walkPass {
	t.Helper()
	pass := walkPass{
		rollout: &shardRolloutTracker{generation: f.shard.Generation},
		before:  before,
	}
	pass.poolsErr = f.r.reconcilePools(t.Context(), f.shard, pass.rollout)
	if pass.poolsErr != nil {
		// A pool error ends the pass.
		return pass
	}
	if between != nil {
		between(t, f)
	}
	f.podLists, f.observations = 0, 0
	pass.walkErr = f.r.reconcileDisruption(t.Context(), f.shard, f.rendered, pass.rollout, before)
	pass.condition = meta.FindStatusCondition(f.shard.Status.Conditions, "RollingUpdate").DeepCopy()
	return pass
}

// pass is reconcile for a test that looks only at the tracker and errors.
func (f *walkFixture) pass(t *testing.T) (*shardRolloutTracker, error, error) {
	t.Helper()
	pass := f.reconcile(t, nil, nil)
	return pass.rollout, pass.poolsErr, pass.walkErr
}

// walkCase is one shard and what a reconcile pass must do with it. The
// defaults are a pool "main" of four Ready pods in cell "a", pod 0 the
// primary, one pass, no error and no pod drained.
type walkCase struct {
	name     string
	pools    []string
	cell     func(pool string) string
	replicas int32
	leader   int
	arrange  func(*testing.T, *walkFixture)
	between  func(*testing.T, *walkFixture)
	passes   int
	poolErr  bool
	walkErr  bool
	drained  []int
	check    func(*testing.T, *walkFixture, walkPass)
}

func runWalkCases(t *testing.T, cases []walkCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pools, cell, replicas := tc.pools, tc.cell, tc.replicas
			if pools == nil {
				pools = []string{"main"}
			}
			if cell == nil {
				cell = func(string) string { return "a" }
			}
			if replicas == 0 {
				replicas = 4
			}
			f := newWalkFixtureIn(t, pools, cell, replicas, tc.leader)
			if tc.arrange != nil {
				tc.arrange(t, f)
			}
			var before *metav1.Condition
			for n := range max(tc.passes, 1) {
				pass := f.reconcile(t, before, tc.between)
				pass.n = n
				if (pass.poolsErr != nil) != tc.poolErr || (pass.walkErr != nil) != tc.walkErr {
					t.Fatalf(
						"pass %d: pool error %v, walk error %v",
						n,
						pass.poolsErr,
						pass.walkErr,
					)
				}
				f.requireDrained(t, tc.drained...)
				if tc.check != nil {
					tc.check(t, f, pass)
				}
				before = pass.condition
			}
		})
	}
}

// all runs several checks on a pass.
func all(
	checks ...func(*testing.T, *walkFixture, walkPass),
) func(*testing.T, *walkFixture, walkPass) {
	return func(t *testing.T, f *walkFixture, pass walkPass) {
		t.Helper()
		for _, check := range checks {
			check(t, f, pass)
		}
	}
}

// requeues requires the pass's rollout to ask for a requeue within [low,
// high], or for none when both are zero.
func requeues(low, high time.Duration) func(*testing.T, *walkFixture, walkPass) {
	return func(t *testing.T, f *walkFixture, pass walkPass) {
		t.Helper()
		if got := f.r.rolloutRequeue(f.shard, pass.rollout); got < low || got > high {
			t.Fatalf("pass %d: requeue after %s, want within [%s, %s]", pass.n, got, low, high)
		}
	}
}

// The backstop is 6 to 9 minutes; these bounds are the owner's 5 to 15.
var backstop = requeues(5*time.Minute, 15*time.Minute)

// reserved requires the pass to have been reserved as its one disruption.
func reserved(t *testing.T, _ *walkFixture, pass walkPass) {
	t.Helper()
	if !pass.rollout.HasStarted() {
		t.Fatalf("pass %d was not reserved", pass.n)
	}
}

// reports requires the RollingUpdate condition, as stored, to have the
// reason and a message with the given prefix and suffix.
func reports(reason, prefix, suffix string) func(*testing.T, *walkFixture, walkPass) {
	return func(t *testing.T, f *walkFixture, _ walkPass) {
		t.Helper()
		stored := &multigresv1alpha1.Shard{}
		if err := f.base.Get(t.Context(), client.ObjectKeyFromObject(f.shard), stored); err != nil {
			t.Fatal(err)
		}
		got := meta.FindStatusCondition(stored.Status.Conditions, "RollingUpdate")
		if got == nil || got.Reason != reason || !strings.HasPrefix(got.Message, prefix) ||
			!strings.HasSuffix(got.Message, suffix) {
			t.Fatalf("stored RollingUpdate = %+v, want %s %q...%q", got, reason, prefix, suffix)
		}
	}
}

// twoCells puts pool alpha in cell a and every other pool in cell b.
func twoCells(pool string) string {
	if pool == "alpha" {
		return "a"
	}
	return "b"
}

// TestWalkCandidates pins which pod a pass removes: the first one, in the
// walk's order, that the one observation allows. A refused candidate does
// not stop the ones after it; the order between kinds (resize restart,
// extra, drifted replica, primary) holds across pools; the primary goes only
// once no other desired pod is drifted, and when it is itself not Ready it
// is tried after the replicas, refused or not, unless one sits in a pool
// whose maintenance failed.
func TestWalkCandidates(t *testing.T) {
	three := []string{"alpha", "beta", "gamma"}
	two := []string{"alpha", "beta"}
	respond := func(f *walkFixture, i int, change func(*md.StatusResponse)) {
		response := proto.Clone(f.responses[i]).(*md.StatusResponse)
		change(response)
		f.responses[i] = response
		f.rpc.SetStatusResponse(topoclient.ComponentIDString(response.ConsensusStatus.Id), response)
	}
	unreadyPrimary := func(drifted ...int) func(*testing.T, *walkFixture) {
		return func(t *testing.T, f *walkFixture) {
			f.drift(t, drifted...)
			f.notReady(t, 0)
		}
	}
	runWalkCases(t, []walkCase{
		{
			// The primary, pod 3, does not report pod 1 connected, so pod
			// 0's removal is refused and pod 1's is not.
			name:   "the pod past a refusal, on one read and one observation",
			leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 0, 1, 2, 3)
				f.connect(0, 2)
			},
			drained: []int{1},
			check: all(reserved, func(t *testing.T, f *walkFixture, pass walkPass) {
				if f.podLists != 1 || f.observations != 1 || pass.rollout.blocked != "" {
					t.Fatalf("%d uncached pod lists, %d observations, blocked %q",
						f.podLists, f.observations, pass.rollout.blocked)
				}
			}),
		},
		{
			name:  "the pod past a refusal in an earlier pool",
			pools: two, replicas: 2,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 1, 2, 3)
				f.connect(1, 3)
			},
			drained: []int{2},
		},
		{
			name:    "one pod per pass",
			leader:  3,
			arrange: func(t *testing.T, f *walkFixture) { f.drift(t, 0, 1, 2, 3) },
			drained: []int{0},
		},
		{
			name:   "nothing read and nothing awaited on a converged shard",
			leader: 3,
			check: all(requeues(0, 0), func(t *testing.T, f *walkFixture, pass walkPass) {
				if f.podLists != 0 || f.observations != 0 || pass.rollout.waitingForRecovery {
					t.Fatalf("%d uncached pod lists, %d observations, waiting=%v; want none",
						f.podLists, f.observations, pass.rollout.waitingForRecovery)
				}
			}),
		},
		{
			name:  "an extra in the later pool before a drifted replica in the earlier",
			pools: two, replicas: 3, leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 0)
				f.scale("beta", 2)
			},
			drained: []int{5},
		},
		{
			name:  "an extra in the earlier pool before a drifted replica in the later",
			pools: two, replicas: 3, leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.scale("alpha", 2)
				f.drift(t, 4)
			},
			drained: []int{2},
		},
		{
			name:  "a resize in the later pool before an extra in the earlier",
			pools: two, replicas: 3, leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.scale("alpha", 2)
				f.resizePending(t, "beta", 1)
			},
			drained: []int{4},
		},
		{
			name:  "a resize in the later pool before a drifted replica in the earlier",
			pools: two, replicas: 3, leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 0)
				f.resizePending(t, "beta", 1)
			},
			drained: []int{4},
		},
		{
			name:  "a resize restart of the drifted primary waits for a later pool",
			pools: three, replicas: 1,
			arrange: func(t *testing.T, f *walkFixture) {
				f.shard.Spec.Images.Postgres = "postgres:updated"
				f.resizePending(t, "alpha", 0)
			},
			drained: []int{1},
		},
		{
			name:     "a current-spec primary resize waits for a replaceable follower",
			pools:    three,
			replicas: 1,
			arrange: func(t *testing.T, f *walkFixture) {
				pool := f.shard.Spec.Pools["gamma"]
				pool.FSGroup = ptr.To(int64(1234))
				f.shard.Spec.Pools["gamma"] = pool
				f.resizePending(t, "alpha", 0)
			},
			drained: []int{2},
		},
		{
			name:     "a resize-only primary restarts",
			pools:    three,
			replicas: 1,
			arrange:  func(t *testing.T, f *walkFixture) { f.resizePending(t, "alpha", 0) },
			drained:  []int{0},
		},
		{
			name:  "an urgent primary resize follows a refused replica",
			pools: []string{"alpha", "beta", "gamma", "omega"}, replicas: 1,
			arrange: func(t *testing.T, f *walkFixture) {
				// Beta is outside the committed three-member cohort. Its newer
				// rule refuses its own removal, not the healthy primary's.
				for i := range f.responses {
					respond(f, i, func(response *md.StatusResponse) {
						rule := response.ConsensusStatus.CurrentPosition.Position.Decision
						rule.CohortMembers = slices.Delete(rule.CohortMembers, 1, 2)
						if i == 1 {
							rule.RuleNumber.CoordinatorTerm++
						}
					})
				}
				pool := f.shard.Spec.Pools["beta"]
				pool.FSGroup = ptr.To(int64(1234))
				f.shard.Spec.Pools["beta"] = pool
				f.resizePending(t, "alpha", 0)
			},
			drained: []int{0},
		},
		{
			name:     "two primary markers do not hide the former leader or restart the observed leader first",
			pools:    three,
			replicas: 1,
			leader:   1,
			arrange: func(t *testing.T, f *walkFixture) {
				f.shard.Status.PodRoles[f.names[0]] = "PRIMARY"
				f.shard.Spec.Images.Postgres = "postgres:updated"
				f.resizePending(t, "alpha", 0)
				f.resizePending(t, "beta", 0)
			},
			drained: []int{0},
		},
		{
			name:     "two primary markers do not deadlock ordinary replacement",
			pools:    three,
			replicas: 1,
			leader:   0,
			arrange: func(_ *testing.T, f *walkFixture) {
				f.shard.Status.PodRoles[f.names[1]] = "PRIMARY"
				f.shard.Spec.Images.Postgres = "postgres:updated"
			},
			drained: []int{1},
		},
		{
			name:  "a replica in a later pool before the primary",
			pools: three, replicas: 1,
			arrange: func(t *testing.T, f *walkFixture) { f.drift(t, 0, 1, 2) },
			drained: []int{1},
		},
		{
			// Topology has moved the primary to beta while the shard's
			// status still names alpha. Removing beta is refused for that,
			// and removing alpha alone would be allowed.
			name:  "the Ready primary waits for a refused replica",
			pools: three, replicas: 1, leader: 1,
			arrange: func(t *testing.T, f *walkFixture) {
				f.shard.Status.PodRoles = map[string]string{f.names[0]: "PRIMARY"}
				f.drift(t, 0, 1)
			},
			check: func(t *testing.T, f *walkFixture, pass walkPass) {
				if !strings.HasPrefix(pass.rollout.blocked, "not removing pod "+f.names[1]+": ") ||
					!pass.rollout.waitingForRecovery {
					t.Fatalf("blocked=%q waiting=%v, want beta's refusal and a poll",
						pass.rollout.blocked, pass.rollout.waitingForRecovery)
				}
			},
		},
		{
			name:  "the primary once nothing else is drifted",
			pools: three, replicas: 1,
			arrange: func(_ *testing.T, f *walkFixture) {
				pool := f.shard.Spec.Pools["alpha"]
				pool.FSGroup = ptr.To(int64(1234))
				f.shard.Spec.Pools["alpha"] = pool
			},
			drained: []int{0},
		},
		{
			name:  "a not-Ready primary with no drifted replica",
			pools: three, replicas: 1,
			arrange: unreadyPrimary(0),
			drained: []int{0},
		},
		{
			// beta is restoring from a backup, work a restart would throw
			// away.
			name:  "a not-Ready primary after a refused replica",
			pools: three, replicas: 1,
			arrange: func(t *testing.T, f *walkFixture) {
				unreadyPrimary(0, 1)(t, f)
				respond(f, 1, func(response *md.StatusResponse) {
					response.Status.PostgresAction = md.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP
				})
			},
			drained: []int{0},
		},
		{
			// Topology has moved the primary to gamma while alpha, which the
			// shard's status still names, runs as primary on the rule
			// before. That refuses every removal but alpha's.
			name:  "a not-Ready primary when the replica's refusal names it",
			pools: three, replicas: 1, leader: 2,
			arrange: func(t *testing.T, f *walkFixture) {
				unreadyPrimary(0, 1)(t, f)
				f.shard.Status.PodRoles = map[string]string{f.names[0]: "PRIMARY"}
				respond(f, 0, func(response *md.StatusResponse) {
					response.Status.PostgresStatus = md.PostgresStatus_POSTGRES_STATUS_PRIMARY
					response.ConsensusStatus.CurrentPosition.Position.Decision.RuleNumber.CoordinatorTerm = 1
				})
			},
			drained: []int{0},
		},
	})
}

// A desired quarantined pod is replaced by the Ready walk if remediation
// hasn't removed it. A surplus ordinal is removed, never replaced.
func TestReadyReplacementDriftScope(t *testing.T) {
	f := newWalkFixture(t, []string{"alpha", "beta"}, 2, 0)
	beta := f.shard.Spec.Pools["beta"]
	beta.FSGroup = ptr.To(int64(1234))
	beta.ReplicasPerCell = ptr.To(int32(1))
	f.shard.Spec.Pools["beta"] = beta
	f.shard.Status.PodRoles[f.names[2]] = "QUARANTINED"
	pods := &corev1.PodList{}
	if err := f.base.List(t.Context(), pods); err != nil {
		t.Fatal(err)
	}
	got := f.r.desiredDrift(f.shard, pods.Items, nil)
	if len(got) != 1 || !got[f.names[2]] {
		t.Fatalf("drift=%v, want only desired quarantined pod %s", got, f.names[2])
	}
}

// Beta's old-template surge is Ready but cannot be removed: it isn't
// registered in the cohort. Alpha has a current peer in its own cell.
func TestPrimaryWaitsOnlyForKeptSurge(t *testing.T) {
	for _, state := range []string{"kept", "retiring", "removed"} {
		t.Run(state, func(t *testing.T) {
			f := newWalkFixtureIn(t, []string{"alpha", "beta", "peer"}, func(pool string) string {
				if pool == "beta" {
					return "b"
				}
				return "a"
			}, 1, 0)
			f.shard.Spec.DurabilityPolicy = multiCellAtLeast2Policy
			spec := f.shard.Spec.Pools["beta"]
			spec.FSGroup = ptr.To(int64(1234))
			if err := f.r.createOrAdoptMaintenanceSurge(
				t.Context(),
				f.shard,
				"beta",
				"b",
				spec,
				nil,
				nil,
				1,
			); err != nil {
				t.Fatal(err)
			}
			f.names = append(f.names, BuildPoolPodName(f.shard, "beta", "b", 1))
			surge := f.pod(t, 3)
			setReady(surge, true)
			if err := f.base.Status().Update(t.Context(), surge); err != nil {
				t.Fatal(err)
			}
			// A real maintenance request keeps the surge; maintenance may act on the
			// request instead of nominating any replacement in beta this pass.
			if state == "kept" {
				f.update(t, 1, func(p *corev1.Pod) {
					p.Annotations[metadata.AnnotationMaintenanceRequested] = maintenanceAnnotationTrue
				})
			}
			if state == "removed" {
				if err := f.base.Delete(t.Context(), surge); err != nil {
					t.Fatal(err)
				}
			}
			alpha := f.shard.Spec.Pools["alpha"]
			alpha.FSGroup = ptr.To(int64(1234))
			f.shard.Spec.Pools["alpha"] = alpha
			pass := f.reconcile(t, nil, nil)
			if pass.walkErr != nil || pass.poolsErr != nil {
				t.Fatalf("maintenance=%v walk=%v", pass.poolsErr, pass.walkErr)
			}
			if state == "kept" {
				f.requireDrained(t)
				if pass.rollout.keptSurges[poolCell{"beta", "b"}] != 1 {
					t.Fatal("maintenance did not keep beta's surge")
				}
				return
			}
			f.requireDrained(t, 0)
		})
	}
}

func TestPrimaryOrderTracksSurgeAdoptedByPoolMaintenance(t *testing.T) {
	f := newWalkFixtureIn(t, []string{"alpha", "beta", "peer"}, func(pool string) string {
		if pool == "beta" {
			return "b"
		}
		return "a"
	}, 1, 0)
	f.shard.Spec.DurabilityPolicy = multiCellAtLeast2Policy

	// beta has one Ready base pod and an old surplus pod that maintenance can
	// adopt instead of creating new capacity. Only the primary has base drift.
	old := f.shard.Spec.Pools["beta"]
	old.FSGroup = ptr.To(int64(1234))
	surplus, err := BuildPoolPod(f.shard, "beta", "b", old, 1, f.r.Scheme)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.Create(t.Context(), surplus); err != nil {
		t.Fatal(err)
	}
	f.names = append(f.names, surplus.Name)
	f.update(t, 1, func(p *corev1.Pod) {
		p.Annotations[metadata.AnnotationMaintenanceRequested] = maintenanceAnnotationTrue
	})
	alpha := f.shard.Spec.Pools["alpha"]
	alpha.FSGroup = ptr.To(int64(1234))
	f.shard.Spec.Pools["alpha"] = alpha

	pass := f.reconcile(t, nil, nil)
	if pass.poolsErr != nil || pass.walkErr != nil {
		t.Fatalf("maintenance=%v walk=%v", pass.poolsErr, pass.walkErr)
	}
	adopted := f.pod(t, 3)
	if !isMaintenanceSurge(adopted) {
		t.Fatal("pool maintenance did not adopt beta's surplus pod")
	}
	if drift := f.r.desiredDrift(f.shard, []corev1.Pod{*adopted}, pass.rollout.keptSurges); !drift[adopted.Name] {
		t.Fatal("primary ordering missed the drift of the surge adopted this pass")
	}
	f.requireDrained(t)
}

// A held cell leaves its replacement for a watch event. No number of
// intervening passes should turn that ordering wait into posture retries.
func TestPrimaryOrderHoldUsesOnlyWatchBackstop(t *testing.T) {
	f := newWalkFixture(t, []string{"alpha", "beta", "gamma"}, 1, 0)
	f.shard.Spec.Images.Postgres = "postgres:next"
	const passes = 40
	var backstops int
	for i := range passes {
		if i == passes/2 {
			f.r.PoolerClients = nil
		} // unavailable RPC must not turn this into a refusal
		rollout := &shardRolloutTracker{pending: true, generation: f.shard.Generation}
		rollout.cell("alpha", "a").rolling = true
		if err := f.r.startDisruption(t.Context(), f.shard, rollout); err != nil {
			t.Fatal(err)
		}
		delay := f.r.rolloutRequeue(f.shard, rollout)
		if delay >= pendingBackstop && !rollout.waitingForRecovery && rollout.blocked == "" {
			backstops++
		}
	}
	if f.observations != 0 {
		t.Fatalf("%d unnecessary observations", f.observations)
	}
	if backstops != passes {
		t.Fatalf("%d of %d holds asked for only a watch backstop", backstops, passes)
	}
	f.requireDrained(t)
}

// TestWalkGates pins what holds every candidate back, and that a request
// from pool maintenance is acted on only while it still describes the
// shard: maintenance reads from its cache at the start of the pass, and the
// walk decides later, on a fresh read.
func TestWalkGates(t *testing.T) {
	two := []string{"alpha", "beta"}
	held := func(polled bool) func(*testing.T, *walkFixture, walkPass) {
		return func(t *testing.T, _ *walkFixture, pass walkPass) {
			if pass.rollout.waitingForRecovery != polled || !pass.rollout.isPending() {
				t.Fatalf("waitingForRecovery=%v pending=%v, want %v and pending",
					pass.rollout.waitingForRecovery, pass.rollout.isPending(), polled)
			}
		}
	}
	bumpStoredGeneration := func(t *testing.T, f *walkFixture) {
		stored := &multigresv1alpha1.Shard{}
		if err := f.base.Get(t.Context(), client.ObjectKeyFromObject(f.shard), stored); err != nil {
			t.Fatal(err)
		}
		stored.Generation++
		if err := f.base.Update(t.Context(), stored); err != nil {
			t.Fatal(err)
		}
	}
	runWalkCases(t, []walkCase{
		{
			name:  "a pod is draining",
			pools: two, replicas: 2, leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 0, 1)
				f.update(t, 2, func(pod *corev1.Pod) {
					pod.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateDraining
				})
			},
			drained: []int{2},
			check:   all(held(false), backstop),
		},
		{
			name:  "a pod is terminating",
			pools: two, replicas: 2, leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 0, 1)
				f.terminate(t, 2)
			},
			check: held(false),
		},
		{
			name:     "the data plane cannot be observed",
			pools:    two,
			replicas: 2,
			leader:   3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 0, 1)
				f.r.PoolerClients = nil
			},
			check: all(
				held(true),
				reports("RolloutBlocked", "cannot observe the data plane: ", ""),
			),
		},
		{
			name:   "another pool pod is not Ready",
			leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 0)
				f.notReady(t, 2)
			},
			check: all(held(false), backstop),
		},
		{
			name:   "a resize restart waits for a healthy shard too",
			leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.resizePending(t, "main", 1)
				f.notReady(t, 2)
			},
		},
		{
			// The data plane phase's status write replaced the in-memory
			// shard with a spec that wants the fourth pod again.
			name:    "the pass holds a newer spec than maintenance read",
			arrange: func(_ *testing.T, f *walkFixture) { f.scale("main", 3) },
			between: func(_ *testing.T, f *walkFixture) {
				f.scale("main", 4)
				f.shard.Generation++
			},
		},
		{
			name:    "the API server holds a newer spec than maintenance read",
			arrange: func(_ *testing.T, f *walkFixture) { f.scale("main", 3) },
			between: bumpStoredGeneration,
			check: func(t *testing.T, _ *walkFixture, pass walkPass) {
				if pass.rollout.HasStarted() {
					t.Fatal("a pass that drained nothing was reserved")
				}
			},
		},
		{
			// A pod recreated under the same name mounted the grown volume
			// when it started.
			name:    "the pod a resize was asked for was replaced",
			leader:  3,
			arrange: func(t *testing.T, f *walkFixture) { f.resizePending(t, "main", 1) },
			between: func(t *testing.T, f *walkFixture) {
				pod := f.pod(t, 1)
				if err := f.base.Delete(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				pod.ResourceVersion, pod.UID = "", "replacement"
				if err := f.base.Create(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				setReady(pod, true)
				if err := f.base.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "the resize a restart was asked for completed",
			leader:  3,
			arrange: func(t *testing.T, f *walkFixture) { f.resizePending(t, "main", 1) },
			between: func(t *testing.T, f *walkFixture) { f.resizeDone(t, "main", 1) },
		},
		{
			// The status write also drops the config hashes the shard carries
			// in memory only. Without them every pod would look drifted and
			// pod 0 would be replaced instead of the one drifted pod.
			name:    "drift is judged on the rendered config after a status write",
			leader:  3,
			arrange: func(t *testing.T, f *walkFixture) { f.drift(t, 2) },
			between: func(t *testing.T, f *walkFixture) {
				stored := &multigresv1alpha1.Shard{}
				if err := f.base.Get(
					t.Context(),
					client.ObjectKeyFromObject(f.shard),
					stored,
				); err != nil {
					t.Fatal(err)
				}
				f.shard.ResourceVersion = stored.ResourceVersion
				if err := f.base.Status().Update(t.Context(), f.shard); err != nil {
					t.Fatal(err)
				}
				if hash := f.shard.Annotations[metadata.AnnotationPostgresConfigHash]; hash != "" {
					t.Fatalf("precondition: the status write kept the config hash %q", hash)
				}
			},
			drained: []int{2},
		},
	})
}

// TestWalkManagedPods pins the one set of pods every gate, count and target
// is over: pool pods the shard controls, named as their labels and ordinal
// give. A pod that only carries the labels holds no gate, supplies no
// capacity, suppresses no surge and asks for nothing; when it holds a
// desired pod's name, the condition says so, the same way on every pass.
func TestWalkManagedPods(t *testing.T) {
	disown := func(pod *corev1.Pod) { pod.OwnerReferences = nil }
	unmanage := map[string]func(*corev1.Pod){
		"not controlled by the shard": disown,
		"named for another pool": func(pod *corev1.Pod) {
			pod.Labels[metadata.LabelMultigresPool] = "other"
		},
	}
	var cases []walkCase
	for how, strip := range unmanage {
		// Pod 4 is beyond the pool's four replicas.
		stray := func(drift bool, put func(*testing.T, *walkFixture)) func(*testing.T, *walkFixture) {
			return func(t *testing.T, f *walkFixture) {
				f.scale("main", 4)
				if drift {
					f.drift(t, 1)
				}
				f.update(t, 4, strip)
				if put != nil {
					put(t, f)
				}
			}
		}
		cases = append(cases,
			walkCase{
				name:     how + ", draining: holds no gate and is not advanced",
				replicas: 5,
				arrange: stray(true, func(t *testing.T, f *walkFixture) {
					f.update(t, 4, func(pod *corev1.Pod) {
						pod.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateRequested
					})
				}),
				drained: []int{1, 4},
				check: func(t *testing.T, f *walkFixture, _ walkPass) {
					if err := f.drainStates(t); err != nil {
						t.Fatal(err)
					}
					if got := f.pod(t, 4).Annotations[metadata.AnnotationDrainState]; got != metadata.DrainStateRequested {
						t.Fatalf("the unmanaged pod's drain state moved to %q", got)
					}
				},
			},
			walkCase{
				name:     how + ", terminating: holds no gate",
				replicas: 5,
				arrange:  stray(true, func(t *testing.T, f *walkFixture) { f.terminate(t, 4) }),
				drained:  []int{1},
			},
			walkCase{
				name:     how + ", converged: nothing read, awaited or reported",
				replicas: 5,
				arrange:  stray(false, nil),
				check: all(
					requeues(0, 0),
					reports("PodsUpToDate", "All pods up to date in pool main", "pool main"),
					func(t *testing.T, f *walkFixture, _ walkPass) {
						if f.podLists != 0 {
							t.Fatalf("%d uncached pod lists, want none", f.podLists)
						}
					},
				),
			},
		)
	}
	// Pool alpha is cell a's only pool, scaled to zero: its last pooler is
	// the cell's last available one, which maintenance capacity keeps.
	capacity := func(strip func(*corev1.Pod)) func(*testing.T, *walkFixture) {
		return func(t *testing.T, f *walkFixture) {
			f.shard.Spec.DurabilityPolicy = multiCellAtLeast2Policy
			f.scale("alpha", 0)
			if strip != nil {
				f.update(t, 1, strip)
				f.update(t, 2, strip)
			}
		}
	}
	unobserved := func(t *testing.T, f *walkFixture, _ walkPass) {
		if f.observations != 0 {
			t.Fatalf("%d observations for a candidate without capacity, want none", f.observations)
		}
	}
	twoCell := []string{"alpha", "beta"}
	cases = append(cases,
		walkCase{
			name:  "capacity: every pooler in the cell is the shard's",
			pools: twoCell, cell: twoCells, replicas: 3, leader: 3,
			arrange: capacity(nil),
			drained: []int{2},
		},
		walkCase{
			name:  "capacity: the cell's other poolers are not controlled by the shard",
			pools: twoCell, cell: twoCells, replicas: 3, leader: 3,
			arrange: capacity(disown),
			check:   unobserved,
		},
		walkCase{
			name:     "capacity: the cell's other poolers are outside the budget's selector",
			pools:    twoCell,
			cell:     twoCells,
			replicas: 3,
			leader:   3,
			arrange: capacity(
				func(pod *corev1.Pod) { delete(pod.Labels, metadata.LabelAppInstance) },
			),
			check: unobserved,
		},
		walkCase{
			// alpha's drifted pod is cell a's only pooler. A Ready pod next
			// to it that the shard does not control is not capacity, so the
			// pass creates the surge the replacement waits for, asks the
			// walk for nothing yet, and waits on the surge's watch events.
			name: "surge: created although an unmanaged pod in the cell is Ready",
			pools: []string{
				"alpha",
				"beta",
				"delta",
				"gamma",
			},
			cell:     twoCells,
			replicas: 1,
			leader:   3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.shard.Spec.DurabilityPolicy = multiCellAtLeast2Policy
				f.drift(t, 0)
				stray := f.pod(t, 0)
				stray.Name = BuildPoolPodName(f.shard, "alpha", "a", 7)
				stray.UID, stray.ResourceVersion, stray.OwnerReferences = "stray", "", nil
				if err := f.base.Create(t.Context(), stray); err != nil {
					t.Fatal(err)
				}
				setReady(stray, true)
				if err := f.base.Status().Update(t.Context(), stray); err != nil {
					t.Fatal(err)
				}
			},
			check: all(backstop, func(t *testing.T, f *walkFixture, _ walkPass) {
				key := client.ObjectKey{
					Namespace: f.shard.Namespace,
					Name:      BuildPoolPodName(f.shard, "alpha", "a", 1),
				}
				if err := f.base.Get(t.Context(), key, &corev1.Pod{}); err != nil {
					t.Fatalf("no maintenance surge was created: %v", err)
				}
			}),
		},
	)
	cases = append(cases,
		walkCase{
			// Pod 1 is desired and the shard does not control it, so the
			// desired pod is missing although pod 4, an extra, makes the
			// pool complete by count. Nothing is removed, maintenance does
			// not try to create over the name, and the condition names the
			// pod, identically on every pass. Nothing watches that pod, so
			// the backstop is what finds it gone.
			name:     "a desired pod's name is held: nothing removed, reported the same every pass",
			replicas: 5,
			arrange: func(t *testing.T, f *walkFixture) {
				f.scale("main", 4)
				f.drift(t, 2)
				f.update(t, 1, disown)
			},
			passes: 3,
			check: all(backstop, func(t *testing.T, f *walkFixture, pass walkPass) {
				want := "desired pod " + f.names[1] +
					" cannot be created: a pod the shard does not manage holds its name" +
					"; ignoring pods the shard does not manage: " + f.names[1]
				got := pass.condition
				if got.Reason != "RolloutBlocked" || got.Message != want {
					t.Fatalf("pass %d: RollingUpdate = %+v, want %q", pass.n, got, want)
				}
				if pass.before != nil && *got != *pass.before {
					t.Fatalf("pass %d: the condition changed between passes", pass.n)
				}
				for recorder := f.r.Recorder.(*record.FakeRecorder); len(recorder.Events) > 0; {
					if event := <-recorder.Events; strings.Contains(event, "PodCreated") {
						t.Fatalf("pass %d: %s", pass.n, event)
					}
				}
			}),
		},
		walkCase{
			// Pod 4, beyond the replica count and not the shard's, mounts a
			// PVC being deleted. Only a managed pod's mount holds the walk.
			name:     "an unmanaged pod mounts a PVC being deleted: holds no gate",
			replicas: 5,
			arrange: func(t *testing.T, f *walkFixture) {
				f.scale("main", 4)
				f.drift(t, 1)
				f.update(t, 4, disown)
				pvc, err := BuildPoolDataPVC(f.shard, "main", "a", f.shard.Spec.Pools["main"], 4,
					false, f.r.Scheme)
				if err != nil {
					t.Fatal(err)
				}
				pvc.Finalizers = []string{"test/hold"}
				if err := f.base.Create(t.Context(), pvc); err != nil {
					t.Fatal(err)
				}
				if err := f.base.Delete(t.Context(), pvc); err != nil {
					t.Fatal(err)
				}
			},
			drained: []int{1},
		},
	)
	runWalkCases(t, cases)
}

// TestWalkWrites pins the walk's one write and what it reports. The drain
// request is bound to the pod as it was judged and to the spec maintenance
// read; a write that returns an error may still have been applied, so it is
// the pass's one disruption and the drift is still reported. A refusal is
// reported in the condition on every pass and as an Event when it changes.
func TestWalkWrites(t *testing.T) {
	three := []string{"alpha", "beta", "gamma"}
	podWrites := 0
	runWalkCases(t, []walkCase{
		{
			// gamma, the primary, reports only beta connected, so removing
			// beta would leave the disconnected alpha among the survivors.
			name:  "every candidate refused: reported, with one Event",
			pools: three, replicas: 1, leader: 2,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 1)
				f.connect(1)
			},
			passes: 2,
			check: func(t *testing.T, f *walkFixture, pass walkPass) {
				reports("RolloutBlocked", "not removing pod "+f.names[1]+": ", "")(t, f, pass)
				events := 0
				for recorder := f.r.Recorder.(*record.FakeRecorder); len(recorder.Events) > 0; {
					if strings.Contains(<-recorder.Events, "DisruptionBlocked") {
						events++
					}
				}
				if again := pass.n > 0; pass.rollout.blockedAgain != again ||
					(events == 0) != again {
					t.Fatalf("pass %d: blockedAgain=%v, %d DisruptionBlocked events",
						pass.n, pass.rollout.blockedAgain, events)
				}
			},
		},
		{
			// The spec changes after the walk read the Shard and before its
			// write lands: the request is withdrawn while it can be.
			name:   "the spec changed while the drain was being requested: withdrawn",
			leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 0)
				f.failWrites(&testutil.FailureConfig{OnPatch: func(obj client.Object) error {
					if obj.GetAnnotations()[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
						return nil
					}
					stored := &multigresv1alpha1.Shard{}
					if err := f.base.Get(
						t.Context(),
						client.ObjectKeyFromObject(f.shard),
						stored,
					); err != nil {
						return err
					}
					stored.Generation++
					return f.base.Update(t.Context(), stored)
				}})
			},
			check: reserved,
		},
		{
			name:   "the drain write fails: no second attempt, and the drift is reported",
			leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 0, 1, 2, 3)
				podWrites = 0
				f.failWrites(&testutil.FailureConfig{OnPatch: func(obj client.Object) error {
					if _, ok := obj.(*corev1.Pod); ok {
						podWrites++
						return errRefused
					}
					return nil
				}})
			},
			walkErr: true,
			check: all(reserved, reports("PodsDrifted", "4 pods need update", ""),
				func(t *testing.T, _ *walkFixture, _ walkPass) {
					if podWrites != 1 {
						t.Fatalf("%d drain writes attempted, want one", podWrites)
					}
				}),
		},
		{
			// The scheduler binding the pod, or a replacement taking its
			// name, between the walk's read and its write.
			name:   "the pod changed since it was judged: not drained, no second attempt",
			leader: 3,
			arrange: func(t *testing.T, f *walkFixture) {
				f.drift(t, 0, 1)
				podWrites = 0
				f.failWrites(&testutil.FailureConfig{OnPatch: func(obj client.Object) error {
					if _, ok := obj.(*corev1.Pod); !ok {
						return nil
					}
					podWrites++
					changed := f.pod(t, 0)
					changed.Spec.NodeName = "another-node"
					return f.base.Update(t.Context(), changed)
				}})
			},
			check: all(reserved, func(t *testing.T, _ *walkFixture, _ walkPass) {
				if podWrites != 1 {
					t.Fatalf("%d drain writes, want one", podWrites)
				}
			}),
		},
	})
}

// TestIgnoredPodsInTheCondition pins how ignored pods are named in the
// RollingUpdate condition: only while something is waiting, sorted, capped.
func TestIgnoredPodsInTheCondition(t *testing.T) {
	rollout := &shardRolloutTracker{}
	for _, name := range []string{"e", "b", "d", "a", "c"} {
		rollout.noteIgnored(name)
	}
	const summary = "; ignoring pods the shard does not manage: a, b, c and 2 more"
	message := func() string {
		var conditions []metav1.Condition
		finishRollingUpdateCondition(&conditions, rollout, nil)
		return meta.FindStatusCondition(conditions, "RollingUpdate").Message
	}
	rollout.notePool("main", 0, false)
	if got := message(); got != "All pods up to date in pool main" {
		t.Fatalf("converged: %q, want no mention of ignored pods", got)
	}
	rollout.notePool("main", 1, true)
	if got := message(); got != "1 pod needs update in pool main"+summary {
		t.Fatalf("drifting: %q", got)
	}
	rollout.block("not removing pod x: refused")
	if got := message(); got != "not removing pod x: refused"+summary {
		t.Fatalf("blocked: %q", got)
	}
}

// TestDrainReason pins the drain state machine's treatment of a requested
// drain on a pod within its replica count and on the desired spec, which it
// otherwise cancels as no longer wanted. A restart for a filesystem resize
// is wanted, but only while its reason names this pod and the pod's data PVC
// still waits for the restart; any other drain request drops an earlier
// reason. A drain step whose write fails is an error: no watch event follows
// a write that did not happen.
func TestDrainReason(t *testing.T) {
	own := func(pod *corev1.Pod) string { return metadata.FilesystemResizeDrainReason(pod.UID) }
	for _, tc := range []struct {
		name string
		// reason is the reason a drain request on pod 1 carries; nil has
		// the walk request the drain for the pending resize.
		reason    func(*corev1.Pod) string
		completed bool
		cancelled bool
	}{
		{name: "requested by the walk for a pending resize"},
		{name: "this pod's reason, resize pending", reason: own},
		{name: "this pod's reason, resize completed", reason: own, completed: true, cancelled: true},
		{
			name:      "another pod's reason, resize pending",
			reason:    func(*corev1.Pod) string { return metadata.FilesystemResizeDrainReason("another") },
			cancelled: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWalkFixture(t, []string{"main"}, 4, 3)
			f.resizePending(t, "main", 1)
			if tc.reason == nil {
				if pass := f.reconcile(t, nil, nil); pass.poolsErr != nil || pass.walkErr != nil {
					t.Fatal(pass.poolsErr, pass.walkErr)
				}
				f.requireDrained(t, 1)
			} else {
				f.update(t, 1, func(pod *corev1.Pod) {
					pod.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateRequested
					pod.Annotations[metadata.AnnotationDrainReason] = tc.reason(pod)
				})
			}
			if tc.completed {
				f.resizeDone(t, "main", 1)
			}
			if err := f.drainStates(t); err != nil {
				t.Fatal(err)
			}
			state := f.pod(t, 1).Annotations[metadata.AnnotationDrainState]
			if cancelled := state == ""; cancelled != tc.cancelled {
				t.Fatalf("drain state %q, want cancelled=%v", state, tc.cancelled)
			}
		})
	}

	// Whether the resize is still pending cannot be read: the drain is
	// neither cancelled nor advanced.
	held := newWalkFixture(t, []string{"main"}, 4, 3)
	held.resizePending(t, "main", 1)
	held.update(t, 1, func(pod *corev1.Pod) {
		pod.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateRequested
		pod.Annotations[metadata.AnnotationDrainReason] = own(pod)
	})
	held.r.APIReader = testutil.NewFakeClientWithFailures(held.base, &testutil.FailureConfig{
		OnGet: func(client.ObjectKey) error { return errRefused },
	})
	if err := held.drainStates(t); !errors.Is(err, errRefused) {
		t.Fatalf("drain state machine returned %v, want the failed read", err)
	}
	if got := held.pod(t, 1).Annotations[metadata.AnnotationDrainState]; got != metadata.DrainStateRequested {
		t.Fatalf("drain state %q after a failed read, want it left requested", got)
	}

	f := newWalkFixture(t, []string{"main"}, 4, 3)
	f.drift(t, 0)
	f.update(
		t,
		0,
		func(pod *corev1.Pod) { pod.Annotations[metadata.AnnotationDrainReason] = own(pod) },
	)
	if pass := f.reconcile(t, nil, nil); pass.poolsErr != nil || pass.walkErr != nil {
		t.Fatal(pass.poolsErr, pass.walkErr)
	}
	f.requireDrained(t, 0)
	if reason, ok := f.pod(t, 0).Annotations[metadata.AnnotationDrainReason]; ok {
		t.Fatalf("the drifted pod's drain kept the earlier reason %q", reason)
	}
	f.failWrites(&testutil.FailureConfig{OnPatch: func(client.Object) error { return errRefused }})
	if err := f.drainStates(t); !errors.Is(err, errRefused) {
		t.Fatalf("drain state machine returned %v, want the failed write", err)
	}
}

// TestRolloutRequeue is the rollout's requeue policy where a pass waits. A
// wait that a watch event ends asks only for the backstop. A refusal only
// the pooler RPC can show the end of is polled on a backoff that grows with
// how long that refusal has lasted, to at least a minute; it starts over
// when the refusal changes or ends, and not when a pass merely stops before
// the checks.
func TestRolloutRequeue(t *testing.T) {
	runWalkCases(t, []walkCase{{
		name:    "a drain requested, then in progress",
		leader:  3,
		arrange: func(t *testing.T, f *walkFixture) { f.drift(t, 0) },
		passes:  2,
		drained: []int{0},
		check: all(backstop, func(t *testing.T, _ *walkFixture, pass walkPass) {
			if pass.rollout.waitingForRecovery {
				t.Fatalf("pass %d asked to be polled", pass.n)
			}
		}),
	}})

	now := time.Unix(1_700_000_000, 0)
	within := func(t *testing.T, got, low, high time.Duration) {
		t.Helper()
		if got < low || got > high {
			t.Fatalf("requeue after %s, want within [%s, %s]", got, low, high)
		}
	}
	f := newWalkFixture(t, []string{"alpha", "beta", "gamma"}, 1, 0)
	f.r.Clock = func() time.Time { return now }
	f.drift(t, 0, 1)
	f.connect(1)
	refusedPass := func() time.Duration {
		t.Helper()
		rollout, poolsErr, err := f.pass(t)
		if poolsErr != nil || err != nil {
			t.Fatal(poolsErr, err)
		}
		if !rollout.waitingForRecovery {
			t.Fatal("the refusal was not polled")
		}
		return f.r.rolloutRequeue(f.shard, rollout)
	}
	// The upper bounds allow readinessBackoffJitter.
	for _, step := range []struct{ elapsed, low, high time.Duration }{
		{0, 5 * time.Second, 6 * time.Second},
		{5 * time.Second, 5 * time.Second, 6 * time.Second},
		{20 * time.Second, 20 * time.Second, 24 * time.Second},
		// A pass a pod event started a moment later does not advance it.
		{21 * time.Second, 21 * time.Second, 26 * time.Second},
		{50 * time.Second, 50 * time.Second, time.Minute},
		{10 * time.Minute, time.Minute, 72 * time.Second},
	} {
		f.r.Clock = func() time.Time { return now.Add(step.elapsed) }
		within(t, refusedPass(), step.low, step.high)
	}

	// A pass that stops before the checks, here on a pod that is not
	// Ready for a moment, does not start the backoff over.
	gamma := f.pod(t, 2)
	setReady(gamma, false)
	if err := f.base.Status().Update(t.Context(), gamma); err != nil {
		t.Fatal(err)
	}
	rollout, poolsErr, err := f.pass(t)
	if poolsErr != nil || err != nil || rollout.waitingForRecovery {
		t.Fatal(poolsErr, err, rollout.waitingForRecovery)
	}
	within(t, f.r.rolloutRequeue(f.shard, rollout), 5*time.Minute, 15*time.Minute)
	gamma = f.pod(t, 2)
	setReady(gamma, true)
	if err := f.base.Status().Update(t.Context(), gamma); err != nil {
		t.Fatal(err)
	}
	within(t, refusedPass(), time.Minute, 72*time.Second)

	// A different refusal starts over.
	f.r.Clock = func() time.Time { return now.Add(11 * time.Minute) }
	clients := f.r.PoolerClients
	f.r.PoolerClients = nil
	within(t, refusedPass(), 5*time.Second, 6*time.Second)
	f.r.PoolerClients = clients

	// So does the same refusal after the rollout was able to move.
	f.connect(1, 2)
	rollout, poolsErr, err = f.pass(t)
	if poolsErr != nil || err != nil {
		t.Fatal(poolsErr, err)
	}
	f.requireDrained(t, 1)
	within(t, f.r.rolloutRequeue(f.shard, rollout), 5*time.Minute, 15*time.Minute)
	f.r.disruptionRefusedMu.Lock()
	tracked := len(f.r.disruptionRefused)
	f.r.disruptionRefusedMu.Unlock()
	if tracked != 0 {
		t.Fatalf("%d refusals still tracked after the rollout moved", tracked)
	}
}

// TestIgnoredPodsAreLoggedOnceAndForgotten pins what logIgnoredPods keeps:
// the unmanaged pods of each pool/cell's latest list, and nothing for a
// shard that is gone.
func TestIgnoredPodsAreLoggedOnceAndForgotten(t *testing.T) {
	f := newWalkFixture(t, []string{"main"}, 4, 3)
	key := ignoredPodsKey{
		shard: types.NamespacedName{Namespace: f.shard.Namespace, Name: f.shard.Name},
		cell:  poolCell{"main", "a"},
	}
	for _, uid := range []types.UID{"first", "second", "third"} {
		stray := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "stray", UID: uid}}
		f.r.logIgnoredPods(t.Context(), f.shard, "main", "a", []corev1.Pod{stray}, nil)
		if kept := f.r.ignoredLogged[key]; len(kept) != 1 {
			t.Fatalf("%d pods remembered after %q replaced the last, want 1", len(kept), uid)
		}
	}
	f.r.forgetStrikes(f.shard.Namespace, f.shard.Name)
	if len(f.r.ignoredLogged) != 0 {
		t.Fatalf("%d pool/cells remembered for a shard that is gone", len(f.r.ignoredLogged))
	}
}
