package shard

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/features"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// dryRunVerdict stands in for the API server's judgement of a pod created as
// a dry run, which the fake client does not validate. It records each probe.
type dryRunVerdict struct {
	// err answers every probe, unless errFor has an answer for the pod.
	err          error
	errFor       map[string]error
	probes       []*corev1.Pod
	statusWrites int
}

func (v *dryRunVerdict) probed(name string) int {
	n := 0
	for _, probe := range v.probes {
		if probe.Name == name {
			n++
		}
	}
	return n
}

func (v *dryRunVerdict) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(
			ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption,
		) error {
			o := &client.CreateOptions{}
			o.ApplyOptions(opts)
			pod, isPod := obj.(*corev1.Pod)
			if !isPod || len(o.DryRun) == 0 {
				return c.Create(ctx, obj, opts...)
			}
			v.probes = append(v.probes, pod.DeepCopy())
			if err, ok := v.errFor[pod.Name]; ok {
				return err
			}
			return v.err
		},
		SubResourcePatch: func(
			ctx context.Context, c client.Client, subResourceName string, obj client.Object,
			patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			if _, isShard := obj.(*multigresv1alpha1.Shard); isShard {
				v.statusWrites++
			}
			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	}
}

func invalidPod(detail string) error {
	return apierrors.NewInvalid(
		schema.GroupKind{Kind: "Pod"}, "pod",
		field.ErrorList{field.Invalid(field.NewPath("spec", "initContainers"), "x", detail)},
	)
}

type replacementFixture struct {
	*poolFixture
	verdict  *dryRunVerdict
	recorder *record.FakeRecorder
}

func newReplacementFixture(t *testing.T) *replacementFixture {
	t.Helper()
	verdict := &dryRunVerdict{}
	f := &replacementFixture{verdict: verdict}
	shard := filesystemBackups(newTestShard())
	pool := newTestPoolSpec()
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	shard.Generation = 1
	scheme := reloadTestScheme(t)
	pod, err := BuildPoolPod(shard, "main", "z1", pool, 0, scheme)
	require.NoError(t, err)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).
		WithStatusSubresource(shard).WithInterceptorFuncs(verdict.funcs()).Build()
	f.recorder = record.NewFakeRecorder(100)
	f.poolFixture = &poolFixture{
		shard: shard, pool: pool, scheme: scheme, pod: pod, client: c,
		reconciler: &ShardReconciler{Client: c, Scheme: scheme, Recorder: f.recorder},
	}
	return f
}

func (f *replacementFixture) rejections() []string {
	var events []string
	for len(f.recorder.Events) > 0 {
		if e := <-f.recorder.Events; strings.Contains(e, "PodTemplateRejected") {
			events = append(events, e)
		}
	}
	return events
}

func (f *replacementFixture) storedCondition(t *testing.T) *metav1.Condition {
	t.Helper()
	got := &multigresv1alpha1.Shard{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.shard), got))
	return meta.FindStatusCondition(
		got.Status.Conditions, multigresv1alpha1.ConditionPodTemplateAccepted,
	)
}

func (f *replacementFixture) drained(t *testing.T) bool {
	t.Helper()
	got := &corev1.Pod{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pod), got))
	return got.Annotations[metadata.AnnotationDrainState] != ""
}

func forbiddenPod(detail string) error {
	return apierrors.NewForbidden(
		schema.GroupResource{Resource: "pods"}, "pod", errors.New(detail))
}

func nameTaken(name string) error {
	return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "pods"}, name)
}

// The probe is the pod that would be created, name, labels and owner
// included, sent as a dry run, so admission judges the object the create
// would send. An answer is one of four things. The API server will take the
// pod: no error, only the name is taken by the pod being replaced, or the
// namespace's quota has no room for it yet. It will not: every other client
// error. It did not judge the pod this time, or it cannot be asked at all;
// neither holds back a pool pod while no refusal is on record.
func TestAskAPIServer_HowAnAnswerIsRead(t *testing.T) {
	conflict := func(reason metav1.StatusReason, kind, name string) error {
		return &apierrors.StatusError{ErrStatus: metav1.Status{
			Status: metav1.StatusFailure, Code: 409, Reason: reason,
			Message: `admission webhook "reservations.example.com" denied the request: taken`,
			Details: &metav1.StatusDetails{Kind: kind, Name: name},
		}}
	}
	status := func(code int32, reason metav1.StatusReason) error {
		return &apierrors.StatusError{ErrStatus: metav1.Status{
			Status: metav1.StatusFailure, Code: code, Reason: reason, Message: "no",
		}}
	}
	const ownName = "own name"
	for name, tc := range map[string]struct {
		err  error
		want replacementVerdict
	}{
		"accepted":                          {nil, replacementAccepted},
		"name taken by the pod it replaces": {nameTaken(ownName), replacementAccepted},
		"another pod's name taken":          {nameTaken("another"), replacementRejected},
		"a config map of that name exists": {apierrors.NewAlreadyExists(
			schema.GroupResource{Resource: "configmaps"}, ownName), replacementRejected},
		"a pod of that name in another API group": {apierrors.NewAlreadyExists(
			schema.GroupResource{Group: "apps", Resource: "pods"}, ownName), replacementRejected},
		"conflict from a webhook": {
			conflict(metav1.StatusReasonConflict, "", ""), replacementRejected,
		},
		"conflict from a webhook naming the pod": {
			conflict(metav1.StatusReasonConflict, "pods", ownName), replacementRejected,
		},
		"invalid": {
			invalidPod("must be a standard resource for containers"), replacementRejected,
		},
		"over a LimitRange maximum": {forbiddenPod(
			"maximum memory usage per Container is 1Gi, but limit is 2Gi"), replacementRejected},
		"against Pod Security": {
			forbiddenPod(
				`violates PodSecurity "restricted:latest": allowPrivilegeEscalation != false`),
			replacementRejected,
		},
		"over a pod quota": {
			forbiddenPod(
				"exceeded quota: q, requested: pods=1, used: pods=3, limited: pods=3"),
			replacementAccepted,
		},
		"over a memory quota by more than the pod it replaces gives back": {
			forbiddenPod("exceeded quota: q, requested: requests.memory=4Gi, " +
				"used: requests.memory=10Gi, limited: requests.memory=10Gi"),
			replacementAccepted,
		},
		"over a quota on two resources": {
			forbiddenPod("exceeded quota: q, requested: count/pods=1,limits.cpu=2, " +
				"used: count/pods=3,limits.cpu=8, limited: count/pods=3,limits.cpu=8"),
			replacementAccepted,
		},
		"no quota covers a resource the cluster requires one for": {
			forbiddenPod("insufficient quota to consume: limits.cpu,pods"), replacementRejected,
		},
		"quota whose usage is not computed yet": {
			forbiddenPod("status unknown for quota: q, resources: pods"), replacementUnanswered,
		},
		"forbidden for another reason that mentions a quota": {
			forbiddenPod("exceeded quota of patience"), replacementRejected,
		},
		"a quota message with another status": {
			apierrors.NewBadRequest(
				"exceeded quota: q, requested: pods=1, used: pods=3, limited: pods=3"),
			replacementRejected,
		},
		"quota that requires a field the pod lacks": {forbiddenPod(
			"failed quota: q: must specify limits.memory"), replacementRejected},
		"denied by a webhook": {apierrors.NewBadRequest(
			`admission webhook "policy.example.com" denied the request: no`), replacementRejected},
		"too large": {apierrors.NewRequestEntityTooLargeError("too big"), replacementRejected},
		"gone":      {status(410, metav1.StatusReasonGone), replacementRejected},
		"webhook without dry-run support": {
			apierrors.NewBadRequest(
				`admission webhook "policy.example.com" does not support dry run`),
			replacementUnaskable,
		},
		"unauthorized": {apierrors.NewUnauthorized("who"), replacementUnanswered},
		"not found": {apierrors.NewNotFound(
			schema.GroupResource{Resource: "namespaces"}, "default"), replacementUnanswered},
		"request timeout": {
			status(408, metav1.StatusReasonTimeout), replacementUnanswered,
		},
		"throttled": {apierrors.NewTooManyRequests("slow down", 1), replacementUnanswered},
		"timeout":   {apierrors.NewTimeoutError("slow", 1), replacementUnanswered},
		"server timeout": {apierrors.NewServerTimeout(
			schema.GroupResource{Resource: "pods"}, "create", 1), replacementUnanswered},
		"unavailable":     {apierrors.NewServiceUnavailable("down"), replacementUnanswered},
		"internal error":  {apierrors.NewInternalError(errors.New("boom")), replacementUnanswered},
		"transport error": {errors.New("connection refused"), replacementUnanswered},
	} {
		t.Run(name, func(t *testing.T) {
			f := newReplacementFixture(t)
			if se, ok := tc.err.(*apierrors.StatusError); ok && se.ErrStatus.Details != nil &&
				se.ErrStatus.Details.Name == ownName {
				se = &apierrors.StatusError{ErrStatus: *se.ErrStatus.DeepCopy()}
				se.ErrStatus.Details.Name = f.pod.Name
				tc.err = se
			}
			f.verdict.err = tc.err

			want, err := BuildPoolPod(f.shard, "main", "z1", f.pool, 0, f.scheme)
			require.NoError(t, err)
			verdict, _ := f.reconciler.askAPIServer(t.Context(), f.shard, f.pod, want)
			assert.Equal(t, tc.want, verdict)

			started, err := f.reconciler.drainForReplacement(t.Context(), f.shard, f.pod)
			require.NoError(t, err)
			assert.Equal(t, tc.want != replacementRejected, started,
				"only a rejection holds a pool pod that has no refusal on record")
			assert.Equal(t, started, f.drained(t))

			require.Len(t, f.verdict.probes, 2)
			assert.Equal(t, want, f.verdict.probes[1],
				"the probe is the pod that would be created, metadata included")
			assert.Equal(t, f.pod.Name, f.verdict.probes[1].Name)

			pods := &corev1.PodList{}
			require.NoError(t, f.client.List(t.Context(), pods))
			assert.Len(t, pods.Items, 1, "a probe creates nothing")
			assert.Equal(t, tc.want == replacementRejected, f.storedCondition(t) != nil)
		})
	}
}

// A dry run that a full quota turns down is counted next to the pod it would
// replace. A pool gives the pod up anyway and is one pod short until there
// is room. A Lite shard has no other pod, so its running pod is kept and the
// answer recorded as a refusal; a Lite pod that has failed or completed
// serves nothing and is replaced.
func TestAskAPIServer_ALiteShardKeepsItsPodAtAFullQuota(t *testing.T) {
	overQuota := forbiddenPod("exceeded quota: q, requested: pods=1, used: pods=3, limited: pods=3")
	for name, tc := range map[string]struct {
		mode  string
		phase corev1.PodPhase
		err   error
		want  replacementVerdict
	}{
		"pool pod":        {"", corev1.PodRunning, overQuota, replacementAccepted},
		"lite, running":   {multigresv1alpha1.ModeLite, corev1.PodRunning, overQuota, replacementRejected},
		"lite, pending":   {multigresv1alpha1.ModeLite, corev1.PodPending, overQuota, replacementRejected},
		"lite, failed":    {multigresv1alpha1.ModeLite, corev1.PodFailed, overQuota, replacementAccepted},
		"lite, completed": {multigresv1alpha1.ModeLite, corev1.PodSucceeded, overQuota, replacementAccepted},
		"lite, room":      {multigresv1alpha1.ModeLite, corev1.PodRunning, nil, replacementAccepted},
		"lite, own name":  {multigresv1alpha1.ModeLite, corev1.PodRunning, nameTaken("own"), replacementAccepted},
		"lite, not computed": {
			multigresv1alpha1.ModeLite, corev1.PodRunning,
			forbiddenPod("status unknown for quota: q, resources: pods"), replacementUnanswered,
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newReplacementFixture(t)
			f.shard.Spec.Mode = tc.mode
			f.pod.Status.Phase = tc.phase
			if se, ok := tc.err.(*apierrors.StatusError); ok && se.ErrStatus.Details != nil &&
				se.ErrStatus.Details.Name == "own" {
				se.ErrStatus.Details.Name = f.pod.Name
			}
			f.verdict.err = tc.err
			want, err := BuildPoolPod(f.shard, "main", "z1", f.pool, 0, f.scheme)
			require.NoError(t, err)

			verdict, err := f.reconciler.judgeReplacement(
				withReplacementPass(t.Context()), f.shard, f.pod, want)
			require.NoError(t, err)
			assert.Equal(t, tc.want, verdict)
			cond := f.storedCondition(t)
			if tc.want != replacementRejected {
				assert.Nil(t, cond)
				return
			}
			require.NotNil(t, cond, "a pod kept for a full quota must be reported")
			assert.Contains(t, cond.Message, "exceeded quota: q")
			assert.Len(t, f.rejections(), 1)
			assert.False(t,
				f.reconciler.quotaWaiting.waiting(client.ObjectKeyFromObject(f.shard)),
				"a dry run is not a create that was turned down")
		})
	}
}

// A request the API server did not judge says nothing about the pod. It does
// not undo a rejection already on record: the pod stays, and so does the
// condition, until the API server gives an answer.
func TestDrainForReplacement_UnansweredQuestionKeepsARecordedRefusal(t *testing.T) {
	for name, unanswered := range map[string]error{
		"server error":    apierrors.NewInternalError(errors.New("failed calling webhook")),
		"throttled":       apierrors.NewTooManyRequests("slow down", 1),
		"transport error": errors.New("connection refused"),
		"no dry-run support": apierrors.NewBadRequest(
			`admission webhook "policy.example.com" does not support dry run`),
	} {
		t.Run(name, func(t *testing.T) {
			f := newReplacementFixture(t)
			ctx := t.Context()
			f.verdict.err = invalidPod("must be greater than or equal to 0")
			started, err := f.reconciler.drainForReplacement(ctx, f.shard, f.pod)
			require.NoError(t, err)
			require.False(t, started)
			recorded := f.storedCondition(t)
			require.NotNil(t, recorded)

			f.verdict.err = unanswered
			for range 3 {
				started, err = f.reconciler.drainForReplacement(ctx, f.shard, f.pod)
				require.NoError(t, err)
				require.False(t, started, "an unanswered question drained a refused pod")
			}
			assert.False(t, f.drained(t))
			assert.Equal(t, recorded, f.storedCondition(t))
			assert.Equal(t, 1, f.verdict.statusWrites)

			// The answer, when it comes, decides.
			f.verdict.err = nil
			started, err = f.reconciler.drainForReplacement(ctx, f.shard, f.pod)
			require.NoError(t, err)
			require.True(t, started)
			assert.Nil(t, f.storedCondition(t))
		})
	}
}

// A replacement that cannot be built is held back like a rejected one: the
// pod that exists is the only one this spec can have.
func TestDrainForReplacement_ReplacementThatCannotBeBuilt(t *testing.T) {
	f := newReplacementFixture(t)
	f.reconciler.Scheme = runtime.NewScheme()
	started, err := f.reconciler.drainForReplacement(t.Context(), f.shard, f.pod)
	require.NoError(t, err)
	assert.False(t, started)
	assert.False(t, f.drained(t))
	assert.Empty(t, f.verdict.probes)
}

// A refusal is one condition and one event however many passes repeat it. It
// is reported again for a new generation of the spec, cleared when the same
// pod's replacement is accepted, and kept when another pod's is.
func TestReplacementAccepted_RefusalLifecycle(t *testing.T) {
	f := newReplacementFixture(t)
	ctx := t.Context()
	f.verdict.err = invalidPod("must be greater than or equal to 0")

	other := f.pod.DeepCopy()
	other.Name = BuildPoolPodName(f.shard, "main", "z1", 1)
	other.ResourceVersion = ""
	require.NoError(t, f.client.Create(ctx, other))

	started, err := f.reconciler.drainForReplacement(ctx, f.shard, f.pod)
	require.NoError(t, err)
	require.False(t, started)
	require.Equal(t, 1, f.verdict.statusWrites)
	first := &multigresv1alpha1.Shard{}
	require.NoError(t, f.client.Get(ctx, client.ObjectKeyFromObject(f.shard), first))
	require.Equal(t, multigresv1alpha1.PhaseDegraded, first.Status.Phase,
		"the phase goes out with the condition, not in a second write")
	// Neither the same refusal again, nor one worded differently, nor a
	// second refused pod writes status: each write would start another pass.
	for i := range 3 {
		f.verdict.err = invalidPod(fmt.Sprintf("must be greater than or equal to 0 (%d)", i))
		for _, pod := range []*corev1.Pod{f.pod, other} {
			started, err := f.reconciler.drainForReplacement(ctx, f.shard, pod)
			require.NoError(t, err)
			require.False(t, started)
		}
	}
	require.Equal(t, 1, f.verdict.statusWrites, "a refusal that stands is written once")
	f.verdict.err = invalidPod("must be greater than or equal to 0")
	require.False(t, f.drained(t))
	cond := f.storedCondition(t)
	require.NotNil(t, cond, "the refusal must be written to status by the pass that makes it")
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, multigresv1alpha1.ReasonPodRejectedByAPIServer, cond.Reason)
	assert.Contains(t, cond.Message, f.pod.Name)
	assert.Contains(t, cond.Message, "must be greater than or equal to 0")
	events := f.rejections()
	require.Len(t, events, 1, "one event for three refused passes")
	assert.Contains(t, events[0], "must be greater than or equal to 0")

	// The status computed at the start of a pass reports the shard Degraded
	// and keeps the refusal while the generation is the one it was made for.
	clearStalePodTemplateRefusal(f.shard)
	f.shard.Status.Phase = multigresv1alpha1.PhaseHealthy
	applyRefusalPhase(f.shard)
	assert.Equal(t, multigresv1alpha1.PhaseDegraded, f.shard.Status.Phase)
	assert.Equal(t, cond.Message, f.shard.Status.Message)

	// Another pod's accepted replacement says nothing about this one.
	f.verdict.errFor = map[string]error{other.Name: nil}
	started, err = f.reconciler.drainForReplacement(ctx, f.shard, other)
	require.NoError(t, err)
	require.True(t, started)
	require.NotNil(t, f.storedCondition(t), "pod 0 is still refused")
	require.Equal(t, 1, f.verdict.statusWrites)

	// A new generation that is still rejected is reported again.
	f.verdict.errFor = nil
	f.verdict.err = invalidPod("must be greater than or equal to 0")
	f.shard.Generation = 2
	clearStalePodTemplateRefusal(f.shard)
	require.Nil(t, meta.FindStatusCondition(
		f.shard.Status.Conditions, multigresv1alpha1.ConditionPodTemplateAccepted,
	), "a refusal does not outlive the generation it was made for")
	f.shard.Status.Phase = multigresv1alpha1.PhaseHealthy
	applyRefusalPhase(f.shard)
	assert.Equal(t, multigresv1alpha1.PhaseHealthy, f.shard.Status.Phase)
	started, err = f.reconciler.drainForReplacement(ctx, f.shard, f.pod)
	require.NoError(t, err)
	require.False(t, started)
	require.Len(t, f.rejections(), 1)

	// Accepted for the same pod: the refusal goes and the pod drains.
	f.verdict.err = nil
	started, err = f.reconciler.drainForReplacement(ctx, f.shard, f.pod)
	require.NoError(t, err)
	require.True(t, started)
	assert.Nil(t, f.storedCondition(t))
	assert.True(t, f.drained(t))
	assert.Empty(t, f.rejections())
}

// A pod whose pool has left the spec is drained to be removed, not replaced,
// so there is no replacement for the API server to judge.
func TestDrainForReplacement_PodWithoutAPlaceInTheSpec(t *testing.T) {
	f := newReplacementFixture(t)
	f.verdict.err = invalidPod("anything")
	delete(f.shard.Spec.Pools, "main")
	started, err := f.reconciler.drainForReplacement(t.Context(), f.shard, f.pod)
	require.NoError(t, err)
	assert.True(t, started)
	assert.Empty(t, f.verdict.probes)
}

// A Failed pool pod is deleted so that its replacement can be created. With
// a replacement the API server rejects, the delete would leave the slot empty
// and take the pod's logs and exit status with it.
func TestCreateMissingResourcesKeepsTerminalPodWithoutAReplacement(t *testing.T) {
	for _, rejected := range []bool{true, false} {
		f := newReplacementFixture(t)
		if rejected {
			f.verdict.err = invalidPod("must be a standard resource for containers")
		}
		f.pod.Status.Phase = corev1.PodFailed
		require.NoError(t, f.client.Status().Update(t.Context(), f.pod))

		_, _, err := f.reconciler.createMissingResources(
			t.Context(), f.shard, "main", "z1", f.pool,
			map[string]*corev1.Pod{f.pod.Name: f.pod},
			map[string]*corev1.PersistentVolumeClaim{}, 1,
		)
		require.NoError(t, err)
		err = f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pod), &corev1.Pod{})
		if rejected {
			require.NoError(t, err, "the Failed pod is kept")
			require.Len(t, f.rejections(), 1)
		} else {
			require.True(t, apierrors.IsNotFound(err), "the Failed pod is replaced: %v", err)
		}
	}
}

// A drifted, Ready replica passes every health gate of a rolling update. The
// API server's verdict on its replacement is the last gate, and a rejected
// replacement leaves the replica undrained.
func TestHandleRollingUpdates_RejectedReplacementIsNotDrained(t *testing.T) {
	for _, rejected := range []bool{true, false} {
		f := newReplacementFixture(t)
		if rejected {
			f.verdict.err = invalidPod("must be greater than or equal to 0")
		}
		f.pod.Annotations[metadata.AnnotationSpecHash] = "old-hash"
		f.pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
		require.NoError(t, f.client.Update(t.Context(), f.pod))
		require.NoError(t, f.client.Status().Update(t.Context(), f.pod))
		observeHealthyDisruption(t, f.reconciler, f.shard, "main", "z1")

		rollout := &shardRolloutTracker{}
		require.NoError(t, f.reconciler.handleRollingUpdates(
			t.Context(), f.shard, "main", "z1", f.shard.Spec.Pools["main"],
			map[string]*corev1.Pod{f.pod.Name: f.pod}, 1, false, false, rollout,
		))
		assert.Equal(t, !rejected, f.drained(t))
		assert.Equal(t, !rejected, rollout.HasStarted())
		require.Len(t, f.verdict.probes, 1)
	}
}

// A drain that the API server agreed to when it started can finish after the
// spec or the admission policy changed. The pod is deleted only when the pod
// that would be created in its place now is still accepted; a refusal is
// recorded and the pod stays. A pod beyond the replica count gets no
// successor, so nothing is asked about it.
func TestHandleScaleDown_DrainedPodIsDeletedOnlyForAnAcceptedReplacement(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rejected   bool
		replicas   int32
		wantDelete bool
		wantProbes int
	}{
		{name: "accepted", replicas: 1, wantDelete: true, wantProbes: 1},
		{name: "rejected since the drain started", rejected: true, replicas: 1, wantProbes: 1},
		{name: "rejected, pod is being removed", rejected: true, replicas: 0, wantDelete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplacementFixture(t)
			f.pod.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateReadyForDeletion
			require.NoError(t, f.client.Update(t.Context(), f.pod))
			if tc.rejected {
				f.verdict.err = invalidPod("must be greater than or equal to 0")
			}

			_, inProgress, err := f.reconciler.handleScaleDown(
				t.Context(), f.shard, "main", f.shard.Spec.Pools["main"],
				map[string]*corev1.Pod{f.pod.Name: f.pod}, tc.replicas, tc.replicas, false,
			)
			require.NoError(t, err)

			assert.Equal(t, tc.wantDelete, len(f.listPods(t)) == 0)
			assert.Len(t, f.verdict.probes, tc.wantProbes)
			if tc.wantDelete {
				assert.Nil(t, f.storedCondition(t))
				return
			}
			assert.True(t, inProgress)
			cond := f.storedCondition(t)
			require.NotNil(t, cond)
			assert.Equal(t, metav1.ConditionFalse, cond.Status)
			assert.Len(t, f.rejections(), 1)
		})
	}
}

// callersIn lists the functions of this package, tests excluded, whose body
// calls a function or method named callee.
func callersIn(t *testing.T, callee string) []string {
	t.Helper()
	names, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var callers []string
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == callee {
					callers = append(callers, fn.Name.Name)
				}
				return true
			})
		}
	}
	slices.Sort(callers)
	return slices.Compact(callers)
}

// Every drain of a pool pod is either for a replacement, which goes through
// drainForReplacement and so through the API server's verdict, or for
// removal. initiateDrain itself asks nothing, so a new caller of it would be
// a disruption path with no check. The removal paths are the ones listed.
func TestInitiateDrainCallers(t *testing.T) {
	removal := []string{
		"drainForReplacement",    // the checked path itself
		"drainUnreadyExtra",      // an extra pod, with no successor
		"handleExternalDeletion", // the pod is already being deleted
		"handleScaleDown",        // an extra pod, with no successor
		"handlePendingDeletion",  // the shard is going away
	}
	slices.Sort(removal)
	assert.Equal(t, removal, callersIn(t, "initiateDrain"),
		"a pod drained to be replaced must go through drainForReplacement")
}

// A delete that is followed by a create of the same pod is a replacement too,
// and has to ask first. Each function that deletes an object is listed with
// whether it replaces a pod; the ones that do must ask the API server, and a
// function not listed is a delete nobody has classified.
func TestDeletesThatReplaceAPodAskFirst(t *testing.T) {
	replaces := map[string]bool{
		"ensureLitePod":          true,  // the Lite pod, for a spec change
		"createMissingResources": true,  // a terminal pool pod
		"wipeQuarantinedPod":     false, // asked by its caller, checked below
		"handleScaleDown":        false, // a pod whose drain already finished
		"handleDeletion":         false, // the shard is being deleted
		"handlePendingDeletion":  false, // the shard is going away
		"cleanupShardPVCs":       false, // the shard is being deleted
		"reconcileShardPDB":      false, // a PodDisruptionBudget
	}
	deleters := callersIn(t, "Delete")
	// Nothing in the package removes objects in bulk or evicts a pod; either
	// would be a delete this list does not see.
	assert.Empty(t, callersIn(t, "DeleteAllOf"))
	assert.Empty(t, callersIn(t, "Evict"))
	assert.Empty(t, callersIn(t, "SubResource"))
	for _, fn := range deleters {
		_, known := replaces[fn]
		assert.True(t, known, "%s deletes an object and is not classified here", fn)
	}
	asks := slices.Concat(
		callersIn(t, "judgeReplacement"),
		callersIn(t, "poolReplacementVerdict"),
		callersIn(t, "poolReplacementAccepted"),
	)
	for fn, replacing := range replaces {
		assert.Contains(t, deleters, fn, "%s no longer deletes anything", fn)
		if replacing {
			assert.Contains(t, asks, fn, "%s replaces a pod without asking", fn)
		}
	}
	assert.Equal(t, []string{"reconcileQuarantineRemediation"}, callersIn(t, "wipeQuarantinedPod"))
	assert.Contains(t, asks, "reconcileQuarantineRemediation",
		"quarantine remediation wipes a pod and its data without asking")
}

// Quarantine remediation deletes a pod and its data so that both are created
// again from a backup. With a pod the API server rejects, nothing would come
// back, so both are kept and remediation reports that it did nothing. The
// wipe cannot be undone, so an unanswered question keeps them as well. A
// question that cannot be asked keeps nothing: no later pass could ask it.
func TestQuarantineRemediationAsksBeforeWiping(t *testing.T) {
	for name, tc := range map[string]struct {
		answer error
		wiped  bool
	}{
		"accepted": {nil, true},
		"rejected": {forbiddenPod(
			"maximum memory usage per Container is 1Gi, but limit is 2Gi"), false},
		"unanswered": {apierrors.NewInternalError(errors.New("failed calling webhook")), false},
		"cannot be asked": {apierrors.NewBadRequest(
			`admission webhook "policy.example.com" does not support dry run`), true},
	} {
		t.Run(name, func(t *testing.T) {
			shard := qrShard()
			shard.Generation = 1
			markBackupHealthy(shard)
			old := time.Now().Add(-time.Hour)
			badPod := qrPod(shard, 1, old, false)
			goodPod := qrPod(shard, 0, old, true)
			badPVC := qrPVC(shard, 1)
			r := qrReconciler(t, shard, badPod, goodPod, badPVC)
			verdict := &dryRunVerdict{err: tc.answer}
			r.Client = interceptor.NewClient(r.Client.(client.WithWatch), verdict.funcs())
			store := qrStoreWithQuarantined(t, shard, 1)

			acted, err := r.reconcileQuarantineRemediation(t.Context(), store, shard)
			require.NoError(t, err)
			require.Len(t, verdict.probes, 1)
			assert.Equal(t, badPod.Name, verdict.probes[0].Name)
			assert.Equal(t, tc.wiped, acted)
			assert.Equal(t, !tc.wiped,
				exists(t, r, &corev1.Pod{}, client.ObjectKeyFromObject(badPod)))
			assert.Equal(t, !tc.wiped,
				exists(t, r, &corev1.PersistentVolumeClaim{}, client.ObjectKeyFromObject(badPVC)))
			assert.Equal(t, name == "rejected", refusedPod(shard) == badPod.Name)
		})
	}
}

// A quarantined pod whose replacement is rejected is passed over, not the
// end of remediation: the next quarantined pod is still restored.
func TestQuarantineRemediationGoesOnPastARefusedPod(t *testing.T) {
	// The order in which quarantined pods are visited is not fixed, so the
	// refused pod has to come first in at least one of several tries.
	for range 20 {
		shard := qrShard()
		shard.Generation = 1
		markBackupHealthy(shard)
		old := time.Now().Add(-time.Hour)
		refused := qrPod(shard, 0, old, false)
		restorable := qrPod(shard, 1, old, false)
		healthy := qrPod(shard, 2, old, true)
		r := qrReconciler(t, shard, refused, restorable, healthy, qrPVC(shard, 0), qrPVC(shard, 1))
		verdict := &dryRunVerdict{errFor: map[string]error{
			refused.Name: invalidPod("must be greater than or equal to 0"),
		}}
		r.Client = interceptor.NewClient(r.Client.(client.WithWatch), verdict.funcs())
		store := qrStoreWithQuarantined(t, shard, 0, 1)

		acted, err := r.reconcileQuarantineRemediation(t.Context(), store, shard)
		require.NoError(t, err)
		require.True(t, acted)
		require.True(t, exists(t, r, &corev1.Pod{}, client.ObjectKeyFromObject(refused)))
		require.False(t, exists(t, r, &corev1.Pod{}, client.ObjectKeyFromObject(restorable)))
	}
}

// A not-Ready pod beyond the replica count is removed, not replaced, so a pod
// template the API server rejects does not keep it.
func TestReplaceUnreadyDriftedPodRemovesExtraDespiteRejectedTemplate(t *testing.T) {
	r, shard, groups, status, publish := unreadyTwoCellFixture(t)
	verdict := &dryRunVerdict{err: invalidPod("must be greater than or equal to 0")}
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), verdict.funcs())
	pool := shard.Spec.Pools["main"]
	pool.ReplicasPerCell = ptr.To(int32(1))
	shard.Spec.Pools["main"] = pool
	status("b", 1).Status.PostgresReady = false
	status("b", 1).Status.PostgresAction = md.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP
	publish()
	rollout := &shardRolloutTracker{}
	_, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout)
	require.NoError(t, err)
	b1 := groups["b"][BuildPoolPodName(shard, "main", "b", 1)]
	assert.True(t, drainRequested(t, r, b1), "blocked: %q", rollout.blocked)
	for _, probe := range verdict.probes {
		assert.NotEqual(t, b1.Name, probe.Name, "an extra has no successor to ask about")
	}
}

// The pool status pass keeps a refusal made for the Shard's generation, with
// the Degraded phase, and drops one made for an earlier generation: after an
// edit that leaves the pods current nothing asks the API server again, so
// the refusal would otherwise stay with a spec it was not about.
func TestUpdateStatus_PodTemplateRefusalFollowsTheGeneration(t *testing.T) {
	f := newReplacementFixture(t)
	ctx := t.Context()
	f.verdict.err = invalidPod("must be greater than or equal to 0")
	started, err := f.reconciler.drainForReplacement(ctx, f.shard, f.pod)
	require.NoError(t, err)
	require.False(t, started)

	require.NoError(t, f.reconciler.updateStatus(ctx, f.shard, renderedConfig{}))
	require.NotNil(t, f.storedCondition(t))
	stored := &multigresv1alpha1.Shard{}
	require.NoError(t, f.client.Get(ctx, client.ObjectKeyFromObject(f.shard), stored))
	assert.Equal(t, multigresv1alpha1.PhaseDegraded, stored.Status.Phase)

	f.shard.Generation = 2
	require.NoError(t, f.reconciler.updateStatus(ctx, f.shard, renderedConfig{}))
	assert.Nil(t, f.storedCondition(t))
	require.NoError(t, f.client.Get(ctx, client.ObjectKeyFromObject(f.shard), stored))
	assert.NotEqual(t, multigresv1alpha1.PhaseDegraded, stored.Status.Phase)
}

// litePodErr drops the retry interval of ensureLitePod for tests that only
// care whether it failed.
func litePodErr(_ bool, err error) error { return err }

// refusalFixture is a pool with three pods, each with its own answer from
// the API server.
type refusalFixture struct {
	*replacementFixture
	a, b, c *corev1.Pod
}

func newRefusalFixture(t *testing.T) *refusalFixture {
	t.Helper()
	f := &refusalFixture{replacementFixture: newReplacementFixture(t)}
	f.a = f.pod
	for i, into := range []**corev1.Pod{&f.b, &f.c} {
		pod := f.pod.DeepCopy()
		pod.Name = BuildPoolPodName(f.shard, "main", "z1", i+1)
		pod.ResourceVersion = ""
		require.NoError(t, f.client.Create(t.Context(), pod))
		*into = pod
	}
	f.verdict.errFor = map[string]error{}
	return f
}

func (f *refusalFixture) ask(t *testing.T, pod *corev1.Pod) bool {
	t.Helper()
	started, err := f.reconciler.drainForReplacement(
		withReplacementPass(t.Context()), f.shard, pod)
	require.NoError(t, err)
	return started
}

func (f *refusalFixture) recorded(t *testing.T) string {
	t.Helper()
	stored := &multigresv1alpha1.Shard{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.shard), stored))
	return refusedPod(stored)
}

// One refusal is on record at a time. It gives way to another pod's refusal
// as soon as its own pod is gone or acceptable, which is found out when the
// other pod is asked about, and it is not rewritten while it stands.
func TestRecordedRefusalGivesWayWhenItsPodIsNoLongerRefused(t *testing.T) {
	limitRange := forbiddenPod("maximum memory usage per Container is 1Gi, but limit is 2Gi")
	policy := forbiddenPod(`violates PodSecurity "restricted:latest"`)

	t.Run("its pod becomes acceptable", func(t *testing.T) {
		f := newRefusalFixture(t)
		f.verdict.errFor[f.a.Name], f.verdict.errFor[f.b.Name] = limitRange, policy
		require.False(t, f.ask(t, f.a))
		for range 3 {
			require.False(t, f.ask(t, f.b))
		}
		require.Equal(t, f.a.Name, f.recorded(t))
		require.Equal(t, 1, f.verdict.statusWrites, "a standing refusal is not rewritten")

		// The LimitRange is edited. Nothing asks about pod a again, but pod b
		// is still asked about on every pass.
		f.verdict.errFor[f.a.Name] = nameTaken(f.a.Name)
		require.False(t, f.ask(t, f.b))
		require.Equal(t, f.b.Name, f.recorded(t))
		assert.Contains(t, f.storedCondition(t).Message, "PodSecurity")
		require.Equal(t, 2, f.verdict.statusWrites)
		require.Len(t, f.rejections(), 2)
		for range 3 {
			require.False(t, f.ask(t, f.b))
		}
		require.Equal(t, 2, f.verdict.statusWrites)
		assert.False(t, f.drained(t), "pod a was only looked at, not drained")
	})

	t.Run("its pod is gone", func(t *testing.T) {
		f := newRefusalFixture(t)
		f.verdict.err = limitRange
		require.False(t, f.ask(t, f.a))
		require.NoError(t, f.client.Delete(t.Context(), f.a))
		require.False(t, f.ask(t, f.b))
		require.Equal(t, f.b.Name, f.recorded(t))
		require.Equal(t, 2, f.verdict.statusWrites)
	})

	t.Run("another pod is accepted", func(t *testing.T) {
		f := newRefusalFixture(t)
		f.verdict.errFor[f.a.Name] = limitRange
		require.False(t, f.ask(t, f.a))
		// Pod a is acceptable by now, and pod b is asked about and accepted:
		// nothing is refused any more.
		f.verdict.errFor[f.a.Name] = nameTaken(f.a.Name)
		require.True(t, f.ask(t, f.b))
		require.Empty(t, f.recorded(t))
		require.Equal(t, 2, f.verdict.statusWrites)
	})

	t.Run("its pod is still refused", func(t *testing.T) {
		f := newRefusalFixture(t)
		f.verdict.errFor[f.a.Name], f.verdict.errFor[f.b.Name] = limitRange, policy
		require.False(t, f.ask(t, f.a))
		require.True(t, f.ask(t, f.c), "an accepted pod is not held by another pod's refusal")
		require.False(t, f.ask(t, f.b))
		require.Equal(t, f.a.Name, f.recorded(t))
		require.Equal(t, 1, f.verdict.statusWrites)
	})

	t.Run("its pod cannot be asked about", func(t *testing.T) {
		f := newRefusalFixture(t)
		f.verdict.errFor[f.a.Name], f.verdict.errFor[f.b.Name] = limitRange, policy
		require.False(t, f.ask(t, f.a))
		f.verdict.errFor[f.a.Name] = apierrors.NewInternalError(errors.New("boom"))
		require.False(t, f.ask(t, f.b))
		require.Equal(t, f.a.Name, f.recorded(t), "an unanswered question clears nothing")
		require.Equal(t, 1, f.verdict.statusWrites)
	})
}

// A pass that replaces no pod asks the API server nothing, so the status
// pass looks at the refusal on record itself. Without that a refusal whose
// cause was fixed outside the spec, for a pod nothing wants to replace any
// more, would keep the shard Degraded until the next spec edit.
func TestUpdateStatus_RecordedRefusalIsLookedAtAgain(t *testing.T) {
	f := newRefusalFixture(t)
	ctx := t.Context()
	f.verdict.err = forbiddenPod("maximum memory usage per Container is 1Gi, but limit is 2Gi")
	require.False(t, f.ask(t, f.a))

	// Still rejected: every status pass asks once and changes nothing.
	for range 3 {
		before := f.verdict.probed(f.a.Name)
		require.NoError(t, f.reconciler.updateStatus(
			withReplacementPass(ctx), f.shard, renderedConfig{}))
		require.Equal(t, before+1, f.verdict.probed(f.a.Name))
		require.Equal(t, f.a.Name, f.recorded(t))
	}
	// A pass that already asked about the pod does not ask again.
	pass := withReplacementPass(ctx)
	_, err := f.reconciler.drainForReplacement(pass, f.shard, f.a)
	require.NoError(t, err)
	before := f.verdict.probed(f.a.Name)
	require.NoError(t, f.reconciler.updateStatus(pass, f.shard, renderedConfig{}))
	require.Equal(t, before, f.verdict.probed(f.a.Name))

	// The API server gives no answer: the refusal stays.
	f.verdict.err = apierrors.NewServiceUnavailable("down")
	require.NoError(t, f.reconciler.updateStatus(
		withReplacementPass(ctx), f.shard, renderedConfig{}))
	require.Equal(t, f.a.Name, f.recorded(t))

	// The LimitRange is edited.
	f.verdict.err = nameTaken(f.a.Name)
	require.NoError(t, f.reconciler.updateStatus(
		withReplacementPass(ctx), f.shard, renderedConfig{}))
	require.Empty(t, f.recorded(t))
	stored := &multigresv1alpha1.Shard{}
	require.NoError(t, f.client.Get(ctx, client.ObjectKeyFromObject(f.shard), stored))
	assert.NotEqual(t, multigresv1alpha1.PhaseDegraded, stored.Status.Phase)

	// And when the pod is gone.
	f.verdict.err = forbiddenPod("maximum memory usage per Container is 1Gi, but limit is 2Gi")
	require.False(t, f.ask(t, f.b))
	require.Equal(t, f.b.Name, f.recorded(t))
	require.NoError(t, f.client.Delete(ctx, f.b))
	require.NoError(t, f.reconciler.updateStatus(
		withReplacementPass(ctx), f.shard, renderedConfig{}))
	require.Empty(t, f.recorded(t))
}

// The status pass runs before the config hashes of the pass are stamped on
// the shard, and pods are built from the shard with them. The pod it asks
// about must still be the pod the pass goes on to create: the answer is
// reused by the rest of the pass.
func TestUpdateStatus_AsksAboutThePodThePassWouldCreate(t *testing.T) {
	f := newRefusalFixture(t)
	ctx := t.Context()
	rendered := reloadTestRendered()
	// What reconcilePostgresConfig does to the shard once the config is
	// delivered.
	stamp := func(shard *multigresv1alpha1.Shard) { stampConfigHashes(shard, rendered) }
	f.verdict.err = forbiddenPod("maximum memory usage per Container is 1Gi, but limit is 2Gi")
	stamp(f.shard)
	require.False(t, f.ask(t, f.a))
	require.Equal(t, f.a.Name, f.recorded(t))

	// The next pass starts from the stored Shard, which has no hashes yet.
	next := &multigresv1alpha1.Shard{}
	require.NoError(t, f.client.Get(ctx, client.ObjectKeyFromObject(f.shard), next))
	require.Empty(t, next.Annotations[metadata.AnnotationPostgresConfigHash])
	f.verdict.probes = nil
	pass := withReplacementPass(ctx)
	require.NoError(t, f.reconciler.updateStatus(pass, next, rendered))
	require.Len(t, f.verdict.probes, 1)

	stamp(next)
	want, err := BuildPoolPod(next, "main", "z1", f.pool, 0, f.scheme)
	require.NoError(t, err)
	assert.Equal(t, want, f.verdict.probes[0])
	_, err = f.reconciler.drainForReplacement(pass, next, f.a)
	require.NoError(t, err)
	assert.Len(t, f.verdict.probes, 1, "the pass asks once about a pod")

	// A config that does not render creates no pod, so nothing is asked.
	f.verdict.probes = nil
	failed := rendered
	failed.err = errors.New("no such ConfigMap")
	require.NoError(t, f.client.Get(ctx, client.ObjectKeyFromObject(f.shard), next))
	require.NoError(t, f.reconciler.updateStatus(withReplacementPass(ctx), next, failed))
	assert.Empty(t, f.verdict.probes)
	assert.Equal(t, f.a.Name, f.recorded(t))
}

// The Lite pod is the shard's only one. It is deleted when the API server
// says it will take the replacement, kept when the API server rejects it,
// and kept, and reported as unanswered, when the API server did not answer.
// A question that cannot be asked does not keep the pod, because asking
// again could not ask it either.
func TestEnsureLitePod_DeletesOnlyForAnAcceptedReplacement(t *testing.T) {
	noDryRun := apierrors.NewBadRequest(
		`admission webhook "policy.example.com" does not support dry run`)
	for name, tc := range map[string]struct {
		// refusedFirst has the replacement rejected in an earlier pass.
		refusedFirst bool
		answer       error
		deleted      bool
		unanswered   bool
	}{
		"accepted": {answer: nil, deleted: true},
		"rejected": {answer: invalidPod("must be greater than or equal to 0")},
		"unanswered": {
			answer: apierrors.NewTooManyRequests("slow down", 1), unanswered: true,
		},
		"cannot be asked":                   {answer: noDryRun, deleted: true},
		"cannot be asked after a rejection": {refusedFirst: true, answer: noDryRun},
		"unanswered after a rejection": {
			refusedFirst: true, answer: apierrors.NewServiceUnavailable("down"),
			unanswered: true,
		},
		"accepted after a rejection": {refusedFirst: true, answer: nil, deleted: true},
	} {
		t.Run(name, func(t *testing.T) {
			shard, pool := newTestLiteShard(), newTestPoolSpec()
			shard.Generation = 1
			scheme := reloadTestScheme(t)
			pod, err := BuildLitePod(shard, "main", "z1", pool, scheme)
			require.NoError(t, err)
			pod.Annotations[metadata.AnnotationSpecHash] = "an-earlier-spec"
			verdict := &dryRunVerdict{err: tc.answer}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).
				WithStatusSubresource(shard).WithInterceptorFuncs(verdict.funcs()).Build()
			recorder := record.NewFakeRecorder(10)
			r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: recorder}

			if tc.refusedFirst {
				verdict.err = invalidPod("must be greater than or equal to 0")
				require.NoError(
					t,
					litePodErr(r.ensureLitePod(t.Context(), shard, "main", "z1", pool)),
				)
				require.Equal(t, pod.Name, refusedPod(shard))
				verdict.err, verdict.probes = tc.answer, nil
				<-recorder.Events
			}

			unanswered, err := r.ensureLitePod(t.Context(), shard, "main", "z1", pool)
			require.NoError(t, err)
			assert.Equal(t, tc.unanswered, unanswered)
			require.Len(t, verdict.probes, 1)
			err = c.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
			assert.Equal(t, tc.deleted, apierrors.IsNotFound(err))
			wantRecorded := name == "rejected" || (tc.refusedFirst && !tc.deleted)
			assert.Equal(t, wantRecorded, refusedPod(shard) == pod.Name)

			// A pod kept for want of an answer is reported, by an event and
			// not by status.
			var events []string
			for len(recorder.Events) > 0 {
				events = append(events, <-recorder.Events)
			}
			reported := slices.ContainsFunc(events, func(e string) bool {
				return strings.Contains(e, "Warning PodReplacementUnanswered pod "+pod.Name)
			})
			assert.Equal(t, tc.unanswered, reported, "events: %q", events)
			stored := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(shard), stored))
			assert.Equal(t, wantRecorded, refusedPod(stored) == pod.Name)
		})
	}
}

// A recorded Lite refusal is looked at again by the status pass, and the
// result is in the status that pass stores: the pass that follows asks the
// API server only when the pod needs replacing and writes nothing when the
// answer is the one on record. Without this a refusal whose cause went away
// outside the spec would keep the shard Degraded until the next spec edit.
func TestUpdateLiteStatus_RecordedRefusalIsLookedAtAgain(t *testing.T) {
	limitRange := forbiddenPod("maximum memory usage per Container is 1Gi, but limit is 2Gi")
	for name, tc := range map[string]struct {
		// then is what happens after the refusal is recorded.
		then    func(t *testing.T, c client.Client, pod *corev1.Pod, verdict *dryRunVerdict)
		cleared bool
	}{
		"still rejected": {
			then: func(*testing.T, client.Client, *corev1.Pod, *dryRunVerdict) {},
		},
		"no answer": {
			then: func(_ *testing.T, _ client.Client, _ *corev1.Pod, v *dryRunVerdict) {
				v.err = apierrors.NewServiceUnavailable("down")
			},
		},
		"cannot be asked": {
			then: func(_ *testing.T, _ client.Client, _ *corev1.Pod, v *dryRunVerdict) {
				v.err = apierrors.NewBadRequest(
					`admission webhook "policy.example.com" does not support dry run`)
			},
		},
		"the cause is gone and the pod was created again on the current spec": {
			then: func(t *testing.T, c client.Client, pod *corev1.Pod, v *dryRunVerdict) {
				current, err := BuildLitePod(
					newTestLiteShard(), "main", "z1", newTestPoolSpec(), reloadTestScheme(t))
				require.NoError(t, err)
				stored := &corev1.Pod{}
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pod), stored))
				stored.Annotations[metadata.AnnotationSpecHash] = current.Annotations[metadata.AnnotationSpecHash]
				require.NoError(t, c.Update(t.Context(), stored))
				v.err = nameTaken(pod.Name)
			},
			cleared: true,
		},
		"the cause is gone": {
			then: func(_ *testing.T, _ client.Client, pod *corev1.Pod, v *dryRunVerdict) {
				v.err = nameTaken(pod.Name)
			},
			cleared: true,
		},
		"the pod is gone": {
			then: func(t *testing.T, c client.Client, pod *corev1.Pod, _ *dryRunVerdict) {
				require.NoError(t, c.Delete(t.Context(), pod))
			},
			cleared: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			shard, pool := newTestLiteShard(), newTestPoolSpec()
			pool.Cells = []multigresv1alpha1.CellName{"z1"}
			shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"main": pool,
			}
			shard.Generation = 1
			scheme := reloadTestScheme(t)
			pod, err := BuildLitePod(shard, "main", "z1", pool, scheme)
			require.NoError(t, err)
			pod.Annotations[metadata.AnnotationSpecHash] = "an-earlier-spec"
			verdict := &dryRunVerdict{err: limitRange}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).
				WithStatusSubresource(shard).WithInterceptorFuncs(verdict.funcs()).Build()
			r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
			stored := func() *multigresv1alpha1.Shard {
				got := &multigresv1alpha1.Shard{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), got))
				return got
			}

			require.NoError(t, litePodErr(r.ensureLitePod(ctx, shard, "main", "z1", pool)))
			require.Equal(t, pod.Name, refusedPod(stored()))
			require.Equal(t, multigresv1alpha1.PhaseDegraded, stored().Status.Phase)

			tc.then(t, c, pod, verdict)
			// What the next pass starts from is what the API server stores.
			next := stored()
			verdict.probes = nil
			rendered := reloadTestRendered()
			require.NoError(t, r.updateLiteStatus(withReplacementPass(ctx), next, rendered))
			if name != "the pod is gone" {
				// The pod asked about is the Lite pod the pass would create,
				// built with the config hashes of the pass.
				asShard, ok := withConfigHashes(next, rendered)
				require.True(t, ok)
				want, err := r.buildLitePod(asShard, "main", "z1", pool)
				require.NoError(t, err)
				require.Len(t, verdict.probes, 1)
				assert.Equal(t, want, verdict.probes[0])
			}
			got := stored()
			if tc.cleared {
				assert.Empty(t, refusedPod(got))
				assert.NotEqual(t, multigresv1alpha1.PhaseDegraded, got.Status.Phase)
				assert.NotContains(t, got.Status.Message, "API server rejects")
			} else {
				assert.Equal(t, pod.Name, refusedPod(got))
				assert.Equal(t, multigresv1alpha1.PhaseDegraded, got.Status.Phase)
			}
		})
	}
}

// settledLite is a Lite shard with nothing left to do but replace its pod,
// which runs on an earlier spec: registered in topology, its data volume at
// its requested size, and primary or not as asked. The API server answers
// every dry run with verdict.err.
type settledLite struct {
	r        *ShardReconciler
	c        client.Client
	shard    *multigresv1alpha1.Shard
	pod      *corev1.Pod
	rendered renderedConfig
	verdict  *dryRunVerdict
	recorder *record.FakeRecorder
}

func newSettledLite(t *testing.T, primary bool, answer error) *settledLite {
	t.Helper()
	ctx := t.Context()
	shard, pool := newTestLiteShard(), newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"z1"}
	shard.Generation = 1
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
		"main": pool,
	}
	if primary {
		meta.SetStatusCondition(&shard.Status.Conditions, metav1.Condition{
			Type:   multigresv1alpha1.ConditionPrimary,
			Status: metav1.ConditionTrue,
			Reason: "TestPrimary",
		})
	}
	rendered := reloadTestRendered()
	shard.Annotations = map[string]string{
		metadata.AnnotationPostgresConfigHash: rendered.restartHash,
		metadata.AnnotationPostgresReloadHash: rendered.reloadHash,
	}
	scheme := reloadTestScheme(t)
	pod, err := BuildLitePod(shard, "main", "z1", pool, scheme)
	require.NoError(t, err)
	pod.UID = "running-pod"
	pod.Annotations[metadata.AnnotationSpecHash] = "an-earlier-spec"
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "192.0.2.10"}
	store, factory := memorytopo.NewServerAndFactory(ctx, "z1")
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.RegisterMultipooler(ctx, &cm.Multipooler{
		Id: &cm.ID{
			Component: cm.ID_MULTIPOOLER, Cell: "z1", Name: BuildPoolServiceID(pod.Name),
		},
		Hostname: pod.Status.PodIP,
		PortMap:  map[string]int32{"grpc": DefaultMultipoolerGRPCPort},
		ShardRef: &cm.ShardRef{
			Database:   string(shard.Spec.DatabaseName),
			TableGroup: string(shard.Spec.TableGroupName),
			Shard:      string(shard.Spec.ShardName),
		},
	}, false))
	// The PVC reports its requested size, so the shard does not wait
	// for storage.
	pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme)
	require.NoError(t, err)
	pvc.Status.Capacity = corev1.ResourceList{
		corev1.ResourceStorage: resource.MustParse(pool.Storage.Size),
	}
	verdict := &dryRunVerdict{err: answer}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, pvc).
		WithStatusSubresource(shard).WithInterceptorFuncs(verdict.funcs()).Build()
	r := &ShardReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20),
		PoolerClients: poolerclient.Static(rpcclient.NewFakeClient()),
		CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			return topoclient.NewWithFactory(
				factory, "", []string{""}, topoclient.NewDefaultTopoConfig()), nil
		},
	}
	return &settledLite{
		r: r, c: c, shard: shard, pod: pod, rendered: rendered, verdict: verdict,
		recorder: r.Recorder.(*record.FakeRecorder),
	}
}

// pass is one reconcile of the shard.
func (l *settledLite) pass(t *testing.T) (reconcile.Result, error) {
	t.Helper()
	return l.r.reconcileLite(
		withReplacementPass(context.WithoutCancel(t.Context())),
		l.shard, l.rendered, storageClassCheck{})
}

// A settled Lite shard whose pod is primary is reconciled again for nothing
// but an unanswered question about the pod's replacement, and then when the
// question may next be asked. A shard that is reconciled again for another
// reason keeps that requeue. Neither writes status, and neither fails the
// pass.
func TestReconcileLite_AsksAgainAboutAnUnansweredReplacement(t *testing.T) {
	unanswered := apierrors.NewInternalError(errors.New("failed calling webhook"))
	for name, tc := range map[string]struct {
		answer     error
		notPrimary bool
		want       time.Duration
	}{
		"unanswered":              {answer: unanswered, want: unansweredAskFloor},
		"unanswered, not primary": {answer: unanswered, notPrimary: true, want: litePrimaryRequeue},
		"rejected":                {answer: invalidPod("must be greater than or equal to 0")},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			l := newSettledLite(t, !tc.notPrimary, tc.answer)
			shard, pod, c, verdict := l.shard, l.pod, l.c, l.verdict
			before := shard.Status.DeepCopy()
			l.r.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
			result, err := l.pass(t)
			require.NoError(t, err)
			require.Len(t, verdict.probes, 1)
			assert.Equal(t, tc.want, result.RequeueAfter)
			if tc.answer == unanswered {
				stored := &multigresv1alpha1.Shard{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), stored))
				require.Len(t, stored.Status.Conditions, len(before.Conditions))
				for i, was := range before.Conditions {
					is := stored.Status.Conditions[i]
					is.LastTransitionTime = was.LastTransitionTime
					assert.Equal(t, was, is, "an unanswered question must not change status")
				}
				assert.Equal(t, before.Phase, stored.Status.Phase)
			}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}))
			if tc.answer == unanswered || tc.notPrimary {
				return
			}
			// Nothing the shard watches reports that admission would accept
			// the pod now, so the whole reconcile of a shard with a recorded
			// refusal, which reconcileLite ends without a requeue, comes
			// back with the recheck.
			stored := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), stored))
			require.NotEmpty(t, refusedPod(stored), "the rejection was not recorded")
			require.NoError(t, c.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name: "multigres-admin-password", Namespace: shard.Namespace,
				},
				Data: map[string][]byte{"password": []byte("x")},
			}))
			// The whole reconcile asks the pooler for its role.
			rpc := rpcclient.NewFakeClient()
			rpc.SetStatusResponse(topoclient.ComponentIDString(&cm.ID{
				Component: cm.ID_MULTIPOOLER, Cell: "z1", Name: BuildPoolServiceID(pod.Name),
			}), liteStatusResponse(cm.PoolerType_PRIMARY))
			l.r.PoolerClients = poolerclient.Static(rpc)
			result, err = l.r.Reconcile(ctx, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(shard),
			})
			require.NoError(t, err)
			assert.GreaterOrEqual(t, result.RequeueAfter, refusalRecheckInterval)
			assert.LessOrEqual(t, result.RequeueAfter,
				refusalRecheckInterval+refusalRecheckInterval/5)

			// A gated Shard is not reconciled, so its pass cannot ask about
			// the refused pod again, and it is reconciled when the gate is
			// lifted. A recheck would come back to the gate every five
			// minutes for as long as the Shard is gated.
			require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
				Name:   shard.Namespace,
				Labels: map[string]string{features.ReconcileLabel: features.Disabled},
			}}))
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), stored))
			meta.SetStatusCondition(&stored.Status.Conditions, metav1.Condition{
				Type:               multigresv1alpha1.ConditionPodTemplateAccepted,
				Status:             metav1.ConditionFalse,
				Reason:             multigresv1alpha1.ReasonPodRejectedByAPIServer,
				Message:            "pod " + pod.Name + ": the API server rejects the pod",
				ObservedGeneration: stored.Generation,
			})
			require.NoError(t, c.Status().Update(ctx, stored))
			require.NotEmpty(t, refusedPod(stored))
			result, err = l.r.Reconcile(ctx, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(shard),
			})
			require.NoError(t, err)
			assert.Zero(t, result.RequeueAfter, "a gated Shard was requeued for its refusal")
		})
	}
}

// A Lite shard whose pod is not primary is reconciled every
// litePrimaryRequeue. While the API server gives no answer about the pod's
// replacement, those passes must not each send the dry run: the wait between
// two asks is as long as the question has been unanswered, so one shard
// asks at 0, 10, 20, 40, 80... seconds, and a fleet of them does not load an
// API server that is failing at the rate of the requeue. The requeue itself
// stays, the pod is kept throughout, and only a dry run that was sent is
// reported.
func TestReconcileLite_SpacesOutUnansweredAsks(t *testing.T) {
	unanswered := apierrors.NewInternalError(errors.New("failed calling webhook"))
	l := newSettledLite(t, false, unanswered)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	l.r.Clock = func() time.Time { return now }
	events := func() int {
		n := 0
		for len(l.recorder.Events) > 0 {
			if strings.Contains(<-l.recorder.Events, "PodReplacementUnanswered") {
				n++
			}
		}
		return n
	}

	var askedAt []time.Duration
	for elapsed := time.Duration(0); elapsed <= 170*time.Second; elapsed += litePrimaryRequeue {
		now = start.Add(elapsed)
		asked := len(l.verdict.probes)
		result, err := l.pass(t)
		require.NoError(t, err)
		require.Equal(t, litePrimaryRequeue, result.RequeueAfter)
		require.NoError(t, l.c.Get(t.Context(), client.ObjectKeyFromObject(l.pod), &corev1.Pod{}),
			"the only pod was deleted without an answer")
		sent := len(l.verdict.probes) - asked
		require.Equal(t, sent, events(), "a dry run that was not sent was reported at %s", elapsed)
		if sent > 0 {
			askedAt = append(askedAt, elapsed)
		}
	}
	assert.Equal(t, []time.Duration{
		0, 10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second,
		160 * time.Second,
	}, askedAt)

	// The question changes: the pod turns terminal. That is asked at once,
	// although the next ask about the running pod was due at 320s, and its
	// waits start over.
	failed := &corev1.Pod{}
	require.NoError(t, l.c.Get(t.Context(), client.ObjectKeyFromObject(l.pod), failed))
	failed.Status.Phase = corev1.PodFailed
	require.NoError(t, l.c.Status().Update(t.Context(), failed))
	for _, tc := range []struct {
		at   time.Duration
		asks bool
	}{{200 * time.Second, true}, {200*time.Second + 500*time.Millisecond, false}, {201 * time.Second, true}} {
		now = start.Add(tc.at)
		asked := len(l.verdict.probes)
		_, err := l.pass(t)
		require.NoError(t, err)
		require.Equal(t, tc.asks, len(l.verdict.probes) > asked, "at %s", tc.at)
		events()
	}
	failed.Status.Phase = corev1.PodRunning
	require.NoError(t, l.c.Status().Update(t.Context(), failed))
	now = start.Add(202 * time.Second)
	_, err := l.pass(t)
	require.NoError(t, err)
	events()

	// The API server answers again. The shard learns it with its next ask,
	// which is due a second after the last one, and the record of the
	// unanswered question goes.
	l.verdict.err = nil
	now = now.Add(500 * time.Millisecond)
	_, err = l.pass(t)
	require.NoError(t, err)
	require.NoError(t, l.c.Get(t.Context(), client.ObjectKeyFromObject(l.pod), &corev1.Pod{}))
	now = now.Add(500 * time.Millisecond)
	_, err = l.pass(t)
	require.NoError(t, err)
	require.True(t, apierrors.IsNotFound(
		l.c.Get(t.Context(), client.ObjectKeyFromObject(l.pod), &corev1.Pod{})),
		"an accepted replacement must go ahead")
	assert.Empty(t, l.r.liteUnanswered.byShard)
}

// A settled Lite shard comes back exactly when its question may be asked
// again, so every pass sends one dry run, and the waits double from
// unansweredAskFloor to unansweredAskCap. No pass fails and none is spent
// on a question that is not asked.
func TestReconcileLite_SettledShardAsksOnItsOwnSchedule(t *testing.T) {
	l := newSettledLite(t, true, apierrors.NewServiceUnavailable("down"))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l.r.Clock = func() time.Time { return now }
	var waits []time.Duration
	for range 13 {
		asked := len(l.verdict.probes)
		result, err := l.pass(t)
		require.NoError(t, err)
		require.Len(t, l.verdict.probes, asked+1, "a pass was spent without asking")
		waits = append(waits, result.RequeueAfter)
		now = now.Add(result.RequeueAfter)
		for len(l.recorder.Events) > 0 {
			<-l.recorder.Events
		}
	}
	want := []time.Duration{time.Second}
	for wait := time.Second; len(want) < 13; wait *= 2 {
		want = append(want, min(wait, unansweredAskCap))
	}
	assert.Equal(t, want, waits)
	assert.Equal(t, unansweredAskCap, waits[12])
}

// The wait has a floor and a cap, an answer of any kind ends it, and a pass
// that ends with no question about a replacement drops the record, so a
// question that comes up later is asked at once.
func TestUnansweredAsks(t *testing.T) {
	shard := types.NamespacedName{Namespace: "ns", Name: "shard"}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(seconds int) time.Time { return start.Add(time.Duration(seconds) * time.Second) }
	noAnswer := errors.New("no answer")
	var asks unansweredAsks
	wait := func(seconds int) time.Duration {
		left, held := asks.wait(shard, "q", at(seconds))
		assert.Equal(t, left > 0, held != nil)
		if held != nil {
			assert.Equal(t, heldAnswer{noAnswer}, held)
		}
		return left
	}
	assert.Zero(t, wait(0))

	asks.note(shard, "q", at(0), noAnswer)
	assert.Equal(t, unansweredAskFloor, wait(0), "the second ask waits for the floor")
	assert.Zero(t, wait(1))
	asks.note(shard, "q", at(3), noAnswer)
	assert.Equal(t, time.Second, wait(5))
	assert.Zero(t, wait(6))
	left, held := asks.wait(types.NamespacedName{Namespace: "ns", Name: "other"}, "q", at(5))
	assert.Zero(t, left)
	assert.NoError(t, held)

	asks.note(shard, "q", at(5000), noAnswer)
	assert.Equal(t, time.Second, wait(5999))
	assert.Zero(t, wait(6000), "the wait is capped at 1000s")

	// Another question is asked at once, and its waits start at the floor.
	left, held = asks.wait(shard, "another question", at(5001))
	assert.Zero(t, left)
	require.NoError(t, held)
	asks.note(shard, "another question", at(5001), noAnswer)
	left, _ = asks.wait(shard, "another question", at(5001))
	assert.Equal(t, unansweredAskFloor, left)
	left, held = asks.wait(shard, "q", at(5001))
	assert.Zero(t, left, "the earlier question is not the one on record")
	require.NoError(t, held)

	asks.note(shard, "another question", at(6000), nil)
	assert.Empty(t, asks.byShard)

	r := &ShardReconciler{}
	r.liteUnanswered.note(shard, "q", at(0), noAnswer)
	asking := withReplacementPass(t.Context())
	asking.Value(replacementPassKey{}).(*replacementPass).asked = true
	r.endPass(asking, shard, nil)
	r.endPass(withReplacementPass(t.Context()), shard, errors.New("failed before the question"))
	assert.Len(t, r.liteUnanswered.byShard, 1)
	r.endPass(withReplacementPass(t.Context()), shard, nil)
	assert.Empty(t, r.liteUnanswered.byShard)
}

// An unanswered answer stands in for one question. The pod asked about is
// identified by UID and phase and its replacement by spec hash, so a pod
// created again under the same name, a pod that turned terminal and a spec
// edit are each another question.
func TestReplacementQuestion(t *testing.T) {
	pod := func(uid types.UID, phase corev1.PodPhase, hash string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod", UID: uid,
				Annotations: map[string]string{metadata.AnnotationSpecHash: hash},
			},
			Status: corev1.PodStatus{Phase: phase},
		}
	}
	old, desired := pod("a", corev1.PodRunning, "old"), pod("", "", "new")
	question := replacementQuestion(old, desired)
	assert.Equal(t, question, replacementQuestion(old.DeepCopy(), desired.DeepCopy()))
	assert.NotEqual(t, question, replacementQuestion(pod("b", corev1.PodRunning, "old"), desired))
	assert.NotEqual(t, question, replacementQuestion(pod("a", corev1.PodFailed, "old"), desired))
	assert.NotEqual(t, question, replacementQuestion(old, pod("", "", "newer")))
	assert.Equal(t, question, replacementQuestion(pod("a", corev1.PodRunning, "older"), desired),
		"the old pod's own hash is not part of the question")
}

// Only a Lite shard's asks are spaced out. A pool pod is replaced on an
// unanswered question, so an answer that stood in for a new dry run would
// let a pod go that the API server now rejects.
func TestAskAPIServer_PoolAsksAreNotSpacedOut(t *testing.T) {
	f := newReplacementFixture(t)
	f.verdict.err = apierrors.NewServiceUnavailable("down")
	want, err := BuildPoolPod(f.shard, "main", "z1", f.pool, 0, f.scheme)
	require.NoError(t, err)
	for range 3 {
		verdict, _ := f.reconciler.askAPIServer(
			withReplacementPass(t.Context()), f.shard, f.pod, want)
		require.Equal(t, replacementUnanswered, verdict)
	}
	assert.Len(t, f.verdict.probes, 3)
	assert.Empty(t, f.reconciler.liteUnanswered.byShard)
}

// What each answer means for the pod, by how much giving it up costs and by
// what is on record.
func TestReplacementProceeds(t *testing.T) {
	const pod, other = "pod-0", "pod-1"
	shardWith := func(recorded string) *multigresv1alpha1.Shard {
		shard := &multigresv1alpha1.Shard{}
		shard.Generation = 3
		if recorded != "" {
			meta.SetStatusCondition(&shard.Status.Conditions, metav1.Condition{
				Type:               multigresv1alpha1.ConditionPodTemplateAccepted,
				Status:             metav1.ConditionFalse,
				Reason:             multigresv1alpha1.ReasonPodRejectedByAPIServer,
				Message:            podTemplateSubject(recorded) + "no",
				ObservedGeneration: 3,
			})
		}
		return shard
	}
	for _, tc := range []struct {
		verdict       replacementVerdict
		recorded      string
		waitForAnswer bool
		want          bool
	}{
		{replacementAccepted, "", true, true},
		{replacementAccepted, pod, true, true},
		{replacementRejected, "", false, false},
		{replacementRejected, other, false, false},
		// No answer: waits where the pod cannot be spared, and anywhere
		// while a refusal is on record for the shard.
		{replacementUnanswered, "", true, false},
		{replacementUnanswered, "", false, true},
		{replacementUnanswered, pod, false, false},
		{replacementUnanswered, other, false, false},
		{replacementUnanswered, other, true, false},
		// Cannot be asked: only this pod's own recorded refusal holds it.
		{replacementUnaskable, "", true, true},
		{replacementUnaskable, "", false, true},
		{replacementUnaskable, other, true, true},
		{replacementUnaskable, other, false, true},
		{replacementUnaskable, pod, true, false},
		{replacementUnaskable, pod, false, false},
	} {
		got := replacementProceeds(tc.verdict, shardWith(tc.recorded), pod, tc.waitForAnswer)
		assert.Equal(t, tc.want, got, "%+v", tc)
	}
}

// One refusal is recorded at a time, so a pool pod can be refused on every
// pass without being the pod on record. A pass in which the API server then
// gives no answer about it must not drain it: the recorded refusal says this
// shard's template is one the API server rejects. A question that cannot be
// asked is different, since only the pod's own recorded refusal answers it.
func TestDrainForReplacement_NoAnswerHoldsWhileAnyRefusalIsRecorded(t *testing.T) {
	for name, tc := range map[string]struct {
		answer  error
		started bool
	}{
		"server error": {apierrors.NewInternalError(errors.New("failed calling webhook")), false},
		"throttled":    {apierrors.NewTooManyRequests("slow down", 1), false},
		"cannot be asked": {apierrors.NewBadRequest(
			`admission webhook "policy.example.com" does not support dry run`), true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newRefusalFixture(t)
			f.verdict.err = invalidPod("must be greater than or equal to 0")
			require.False(t, f.ask(t, f.a))
			for range 3 {
				require.False(t, f.ask(t, f.b))
			}
			require.Equal(t, f.a.Name, f.recorded(t))

			f.verdict.errFor[f.b.Name] = tc.answer
			assert.Equal(t, tc.started, f.ask(t, f.b))
			got := &corev1.Pod{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.b), got))
			assert.Equal(t, tc.started, got.Annotations[metadata.AnnotationDrainState] != "")
			assert.Equal(t, f.a.Name, f.recorded(t))
		})
	}
}

func TestLiteRequeue(t *testing.T) {
	for name, tc := range map[string]struct {
		reload  time.Duration
		primary bool
		pvc     time.Duration
		want    time.Duration
	}{
		"settled":             {primary: true},
		"waiting for the PVC": {primary: true, pvc: time.Minute, want: time.Minute},
		"not primary":         {pvc: time.Minute, want: litePrimaryRequeue},
		"reloading":           {reload: 3 * time.Second, pvc: time.Minute, want: 3 * time.Second},
	} {
		assert.Equal(t, tc.want, liteRequeue(tc.reload, tc.primary, tc.pvc), name)
	}
}

// A refusal can have a cause no event of the Shard's reports: an admission
// webhook or policy that changed its mind. A Shard with a refusal on record
// for its current generation is therefore reconciled again after the recheck
// interval, which asks the API server about the pod again, and no other Shard
// is.
func TestAfterRefusalRequeuesOnlyARefusedShard(t *testing.T) {
	refused := func(generation, observed int64) *multigresv1alpha1.Shard {
		shard := &multigresv1alpha1.Shard{ObjectMeta: metav1.ObjectMeta{Generation: generation}}
		meta.SetStatusCondition(&shard.Status.Conditions, metav1.Condition{
			Type:               multigresv1alpha1.ConditionPodTemplateAccepted,
			Status:             metav1.ConditionFalse,
			Reason:             multigresv1alpha1.ReasonPodRejectedByAPIServer,
			Message:            "pod p-0: the API server rejects the pod",
			ObservedGeneration: observed,
		})
		return shard
	}
	for range 50 {
		got := afterRefusal(reconcile.Result{}, refused(3, 3))
		assert.GreaterOrEqual(t, got.RequeueAfter, refusalRecheckInterval)
		assert.LessOrEqual(t, got.RequeueAfter, refusalRecheckInterval+refusalRecheckInterval/5)
	}
	sooner := reconcile.Result{RequeueAfter: time.Second}
	assert.Equal(t, sooner, afterRefusal(sooner, refused(3, 3)),
		"a pass that asked to be run sooner keeps its requeue")
	// A requeue inside the jitter's range is never moved later.
	inRange := refusalRecheckInterval + 10*time.Second
	for range 50 {
		got := afterRefusal(reconcile.Result{RequeueAfter: inRange}, refused(3, 3))
		assert.LessOrEqual(t, got.RequeueAfter, inRange)
	}
	later := afterRefusal(reconcile.Result{RequeueAfter: time.Hour}, refused(3, 3))
	assert.LessOrEqual(t, later.RequeueAfter, refusalRecheckInterval+refusalRecheckInterval/5)
	assert.Equal(
		t,
		reconcile.Result{},
		afterRefusal(reconcile.Result{}, &multigresv1alpha1.Shard{}),
	)
	assert.Equal(t, reconcile.Result{}, afterRefusal(reconcile.Result{}, refused(4, 3)),
		"a refusal of an earlier generation is about another spec")
}

// A LimitRange changes what the API server says about a pod, and it is not
// an object a Shard owns. The event
// reconciles the Shards of that namespace that have a refusal on record, and
// only those.
func TestEnqueueRefusedShards(t *testing.T) {
	scheme := testScheme()
	shard := func(namespace, name string, refused bool) *multigresv1alpha1.Shard {
		s := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Generation: 1},
		}
		if refused {
			meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
				Type:               multigresv1alpha1.ConditionPodTemplateAccepted,
				Status:             metav1.ConditionFalse,
				Reason:             multigresv1alpha1.ReasonPodRejectedByAPIServer,
				Message:            "pod p-0: the API server rejects the pod",
				ObservedGeneration: 1,
			})
		}
		return s
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		shard("ns", "b-refused", true), shard("ns", "a-refused", true),
		shard("ns", "accepted", false), shard("other", "refused-elsewhere", true),
	).WithStatusSubresource(&multigresv1alpha1.Shard{}).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme}
	got := r.enqueueRefusedShards(t.Context(), &metav1.PartialObjectMetadata{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "limits"},
	})
	want := []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "a-refused"}},
		{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "b-refused"}},
	}
	assert.Equal(t, want, got)
}
