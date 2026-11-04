//go:build integration
// +build integration

package shard

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	podresources "k8s.io/component-helpers/resource"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// rejectedSidecarResources are pgbackrestResources blocks the Shard CRD
// stores and pod validation rejects, each with the API server's complaint.
var rejectedSidecarResources = []struct {
	name      string
	resources corev1.ResourceRequirements
	complaint string
}{
	{
		name: "limit below request",
		resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
		},
		complaint: "must be less than or equal to memory limit",
	},
	{
		name: "misspelled resource name",
		resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{"memroy": resource.MustParse("1Gi")},
		},
		complaint: "must be a standard resource for containers",
	},
	{
		name: "negative quantity",
		resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("-1Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
		},
		complaint: "must be greater than or equal to 0",
	},
	{
		name: "resource claim",
		resources: corev1.ResourceRequirements{
			Claims: []corev1.ResourceClaim{{Name: "gpu"}},
		},
		complaint: "pod.spec.resourceClaims",
	},
}

// A pod is replaced by deleting or draining it and creating its successor
// afterwards. The API server validates a pod's resources but not a Shard's,
// so a pgbackrestResources block it will reject is stored without complaint
// (no webhook runs here, and a ShardTemplate edit reaches a Shard without
// cluster admission) and would be turned down only when the successor is
// created, after the old pod is gone. The controller must ask first and keep
// the pod, for a Lite shard's only pod and for a pool pod alike.
//
// The Lite half goes through whole Reconcile calls. Nothing in this package
// makes a pool's pods Ready, which every pool rollout waits for, so the pool
// half calls the step all pool replacements go through; the multi-controller
// suite covers the whole pool path.
func TestPodsSurviveSidecarResourcesTheAPIServerRejects(t *testing.T) {
	cfg := testutil.SetUpEnvtest(
		t,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	scheme := testScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = admissionregistrationv1.AddToScheme(scheme)
	direct, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	// statusApplies collects the body of every status apply to a Shard.
	var statusApplies []string
	// lastProbe is, per namespace, the pod the controller last asked the API
	// server about by dry run: the pod it would create there now.
	lastProbe := map[string]*corev1.Pod{}
	c := interceptor.NewClient(direct, interceptor.Funcs{
		Create: func(
			ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption,
		) error {
			o := &client.CreateOptions{}
			o.ApplyOptions(opts)
			if pod, isPod := obj.(*corev1.Pod); isPod && len(o.DryRun) > 0 {
				lastProbe[pod.Namespace] = pod.DeepCopy()
			}
			return c.Create(ctx, obj, opts...)
		},
		SubResourcePatch: func(
			ctx context.Context, c client.Client, subResourceName string, obj client.Object,
			patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			if _, isShard := obj.(*multigresv1alpha1.Shard); isShard {
				body, err := patch.Data(obj)
				if err != nil {
					return err
				}
				statusApplies = append(statusApplies, string(body))
			}
			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	})
	ctx := context.Background()
	passwordSecret := func(t *testing.T, namespace string) {
		t.Helper()
		require.NoError(t, c.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "multigres-admin-password", Namespace: namespace},
			Data:       map[string][]byte{PostgresPasswordSecretKey: []byte("pw")},
		}))
	}
	passwordSecret(t, "default")

	storedShardIn := func(
		t *testing.T, namespace, name string, lite bool,
		with ...func(*multigresv1alpha1.Shard),
	) *multigresv1alpha1.Shard {
		t.Helper()
		obj := storedByPreviousOperator(name, lite)
		obj.SetNamespace(namespace)
		require.NoError(t, c.Create(ctx, obj))
		shard := &multigresv1alpha1.Shard{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), shard))
		shard.Spec.Images.PgBackRestd = "ghcr.io/multigres/multigres:new"
		shard.Spec.Backup.Filesystem.Storage.Class = ""
		shard.Spec.SidecarBackupLocation = shard.Spec.Backup.DeepCopy()
		for _, change := range with {
			change(shard)
		}
		require.NoError(t, c.Update(ctx, shard))
		return shard
	}
	storedShard := func(t *testing.T, name string, lite bool) *multigresv1alpha1.Shard {
		t.Helper()
		return storedShardIn(t, "default", name, lite)
	}
	// requireQuiet runs further passes over a refusal that stands and
	// requires that none of them changes what is written to the Shard: the
	// controller watches its own Shard, so a pass that changes it starts the
	// next one. Each pass must send the same status applies as the one before
	// and leave the same status, which makes each of them a no-op for the API
	// server. The resourceVersion may still move once: an apply identical to
	// what is stored is occasionally stored again with a new managedFields
	// timestamp, with or without a refusal. The refusal must not add a
	// requeue but its recheck (see afterRefusal); steady is what a pass
	// returned before the template was rejected.
	requireQuiet := func(
		t *testing.T, shard *multigresv1alpha1.Shard, steady *ctrl.Result,
		pass func() ctrl.Result,
	) {
		t.Helper()
		const passes = 6
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), shard))
		version, status := shard.ResourceVersion, shard.Status.DeepCopy()
		moved := 0
		var first []string
		for i := range passes {
			statusApplies = nil
			result := pass()
			if i == 0 {
				first = statusApplies
			}
			require.Equal(t, first, statusApplies,
				"every pass over a refusal that stands must apply the same status")
			if steady != nil {
				// The one requeue a refusal adds is its recheck, which a
				// pass that already asked to run sooner does not need.
				want := *steady
				if want.RequeueAfter == 0 || want.RequeueAfter > refusalRecheckInterval {
					require.GreaterOrEqual(t, result.RequeueAfter, refusalRecheckInterval)
					require.LessOrEqual(t, result.RequeueAfter,
						refusalRecheckInterval+refusalRecheckInterval/5)
					want.RequeueAfter = result.RequeueAfter
				}
				require.Equal(t, want, result,
					"a refusal must add nothing but its recheck requeue")
			}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), shard))
			require.Equal(t, *status, shard.Status,
				"a refusal that stands must not change the Shard's status")
			if shard.ResourceVersion != version {
				moved++
				version = shard.ResourceVersion
			}
		}
		require.LessOrEqual(t, moved, 1, "the Shard is written on every refused pass")
	}
	setSidecarResources := func(
		t *testing.T, shard *multigresv1alpha1.Shard, res corev1.ResourceRequirements,
	) {
		t.Helper()
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), shard))
		pool := shard.Spec.Pools["default"]
		pool.PgBackRestResources = res
		shard.Spec.Pools["default"] = pool
		require.NoError(t, c.Update(ctx, shard),
			"the API server stores a Shard whose sidecar resources a pod could not have")
	}
	livePod := func(t *testing.T, key types.NamespacedName) *corev1.Pod {
		t.Helper()
		pod := &corev1.Pod{}
		err := c.Get(ctx, key, pod)
		if apierrors.IsNotFound(err) {
			return nil
		}
		require.NoError(t, err)
		if !pod.DeletionTimestamp.IsZero() {
			return nil
		}
		return pod
	}
	refusal := func(t *testing.T, shard *multigresv1alpha1.Shard) *metav1.Condition {
		t.Helper()
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), shard))
		return meta.FindStatusCondition(
			shard.Status.Conditions, multigresv1alpha1.ConditionPodTemplateAccepted,
		)
	}
	rejections := func(recorder *record.FakeRecorder) (events []string) {
		for len(recorder.Events) > 0 {
			if e := <-recorder.Events; containsAll(e, "PodTemplateRejected") {
				events = append(events, e)
			}
		}
		return events
	}
	eventsOf := func(recorder *record.FakeRecorder, reason string) (events []string) {
		for len(recorder.Events) > 0 {
			if e := <-recorder.Events; containsAll(e, reason) {
				events = append(events, e)
			}
		}
		return events
	}
	// fullQuota creates a ResourceQuota whose usage equals its limits and
	// waits until admission rejects a pod like probe for it. Envtest runs no
	// quota controller, so the usage is written by hand, here and wherever a
	// test releases what a deleted pod held. That is a gap in the harness
	// (testkit has no ResourceQuota usage sim): a test that forgets a
	// release sees a quota that never frees anything.
	fullQuota := func(
		t *testing.T, namespace, name string, hard corev1.ResourceList, probe *corev1.Pod,
	) *corev1.ResourceQuota {
		t.Helper()
		quota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec:       corev1.ResourceQuotaSpec{Hard: hard},
		}
		require.NoError(t, c.Create(ctx, quota))
		quota.Status = corev1.ResourceQuotaStatus{Hard: hard, Used: hard}
		require.NoError(t, c.Status().Update(ctx, quota))
		require.Eventually(t, func() bool {
			pod := probe.DeepCopy()
			pod.ObjectMeta = metav1.ObjectMeta{Namespace: namespace, Name: "quota-probe"}
			err := c.Create(ctx, pod, client.DryRunAll)
			return apierrors.IsForbidden(err) &&
				strings.Contains(err.Error(), "exceeded quota: "+name)
		}, 10*time.Second, 100*time.Millisecond, "the quota never took effect")
		return quota
	}
	// releaseQuota writes usage as the quota's usage, which the quota
	// controller would do after a pod is gone.
	releaseQuota := func(t *testing.T, quota *corev1.ResourceQuota, usage corev1.ResourceList) {
		t.Helper()
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(quota), quota))
		quota.Status.Used = usage
		require.NoError(t, c.Status().Update(ctx, quota))
	}
	valid := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
	}

	t.Run("lite", func(t *testing.T) {
		shard := storedShard(t, "lite-resources", true)
		recorder := record.NewFakeRecorder(500)
		r := &ShardReconciler{
			Client: c, APIReader: c, Scheme: scheme, Recorder: recorder,
			CreateTopoStore: newMemoryTopoFactory(),
		}
		podKey := types.NamespacedName{
			Namespace: "default", Name: BuildPoolPodName(shard, "default", "zone1", 0),
		}
		_, err := r.Reconcile(ctx, requestOf(shard))
		original := livePod(t, podKey)
		require.NotNil(t, original, "the first reconcile must create the Lite pod: %v", err)
		steady, err := r.Reconcile(ctx, requestOf(shard))
		require.NoError(t, err)

		for _, tc := range rejectedSidecarResources {
			t.Run(tc.name, func(t *testing.T) {
				rejections(recorder)
				setSidecarResources(t, shard, tc.resources)
				for range 3 {
					_, err := r.Reconcile(ctx, requestOf(shard))
					require.NoError(t, err, "a refused replacement is not retried as an error")
				}
				requireQuiet(t, shard, &steady, func() ctrl.Result {
					result, err := r.Reconcile(ctx, requestOf(shard))
					require.NoError(t, err)
					return result
				})
				after := livePod(t, podKey)
				require.NotNil(
					t,
					after,
					"the only pod was deleted for a pod that cannot be created",
				)
				assert.Equal(t, original.UID, after.UID)

				cond := refusal(t, shard)
				require.NotNil(t, cond)
				assert.Equal(t, metav1.ConditionFalse, cond.Status)
				assert.Equal(t, multigresv1alpha1.ReasonPodRejectedByAPIServer, cond.Reason)
				assert.Contains(t, cond.Message, tc.complaint)
				assert.Contains(t, cond.Message, `Pod "`+podKey.Name+`" is invalid`,
					"the API server's answer must be about the pod being replaced")
				assert.Equal(t, multigresv1alpha1.PhaseDegraded, shard.Status.Phase)
				assert.Contains(t, shard.Status.Message, tc.complaint)
				events := rejections(recorder)
				require.Len(t, events, 1, "one event for every refused pass together")
				assert.Contains(t, events[0], tc.complaint)
			})
		}

		// Taking the block out again leaves the pod current, so nothing asks
		// the API server anything. The refusal must still go.
		setSidecarResources(t, shard, corev1.ResourceRequirements{})
		_, err = r.Reconcile(ctx, requestOf(shard))
		require.NoError(t, err)
		assert.Nil(t, refusal(t, shard), "a refusal must not outlive the spec it was about")
		assert.NotEqual(t, multigresv1alpha1.PhaseDegraded, shard.Status.Phase)
		kept := livePod(t, podKey)
		require.NotNil(t, kept)
		assert.Equal(t, original.UID, kept.UID)

		// The control: a block the API server accepts does replace the pod.
		setSidecarResources(t, shard, valid)
		_, _ = r.Reconcile(ctx, requestOf(shard))
		if replaced := livePod(t, podKey); replaced != nil {
			assert.NotEqual(t, original.UID, replaced.UID,
				"a valid resources change replaces the pod")
		}
		assert.Nil(t, refusal(t, shard))
	})

	t.Run("pool", func(t *testing.T) {
		shard := storedShard(t, "pool-resources", false)
		recorder := record.NewFakeRecorder(500)
		r := &ShardReconciler{Client: c, APIReader: c, Scheme: scheme, Recorder: recorder}
		pod, err := BuildPoolPod(shard, "default", "zone1", shard.Spec.Pools["default"], 0, scheme)
		require.NoError(t, err)
		require.NoError(t, c.Create(ctx, pod))
		podKey := client.ObjectKeyFromObject(pod)
		original := livePod(t, podKey)
		require.NotNil(t, original)

		for _, tc := range rejectedSidecarResources {
			t.Run(tc.name, func(t *testing.T) {
				rejections(recorder)
				setSidecarResources(t, shard, tc.resources)
				require.True(t, podNeedsUpdate(original, shard, "default", "zone1",
					shard.Spec.Pools["default"], 0, scheme),
					"the block changes the pod's hash, so a rollout would pick the pod")
				for range 3 {
					started, err := r.drainForReplacement(ctx, shard, livePod(t, podKey))
					require.NoError(t, err)
					require.False(t, started)
				}
				requireQuiet(t, shard, nil, func() ctrl.Result {
					started, err := r.drainForReplacement(ctx, shard, livePod(t, podKey))
					require.NoError(t, err)
					require.False(t, started)
					return ctrl.Result{}
				})
				after := livePod(t, podKey)
				require.NotNil(t, after)
				assert.Equal(t, original.UID, after.UID)
				assert.Empty(t, after.Annotations[metadata.AnnotationDrainState],
					"a pod whose replacement is rejected must not be drained")
				cond := refusal(t, shard)
				require.NotNil(t, cond)
				assert.Contains(t, cond.Message, tc.complaint)
				require.Len(t, rejections(recorder), 1)
			})
		}

		setSidecarResources(t, shard, valid)
		started, err := r.drainForReplacement(ctx, shard, livePod(t, podKey))
		require.NoError(t, err)
		require.True(t, started, "a valid resources change drains the pod")
		assert.Nil(t, refusal(t, shard))
		assert.Equal(t, metadata.DrainStateRequested,
			livePod(t, podKey).Annotations[metadata.AnnotationDrainState])
	})

	// A LimitRange rejects a pod with 403, not 422, and the pod is one the
	// CRD and cluster admission have no objection to. It is as final as a
	// validation error: no pod with that template is admitted, whether or not
	// the old one is gone.
	t.Run("lite pod over a LimitRange maximum", func(t *testing.T) {
		const namespace = "limit-range"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		}))
		passwordSecret(t, namespace)
		shard := storedShardIn(t, namespace, "limited", true)
		recorder := record.NewFakeRecorder(500)
		r := &ShardReconciler{
			Client: c, APIReader: c, Scheme: scheme, Recorder: recorder,
			CreateTopoStore: newMemoryTopoFactory(),
		}
		podKey := types.NamespacedName{
			Namespace: namespace, Name: BuildPoolPodName(shard, "default", "zone1", 0),
		}
		_, err := r.Reconcile(ctx, requestOf(shard))
		original := livePod(t, podKey)
		require.NotNil(t, original, "the first reconcile must create the Lite pod: %v", err)
		steady, err := r.Reconcile(ctx, requestOf(shard))
		require.NoError(t, err)

		require.NoError(t, c.Create(ctx, &corev1.LimitRange{
			ObjectMeta: metav1.ObjectMeta{Name: "max", Namespace: namespace},
			Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
				Type: corev1.LimitTypeContainer,
				Max:  corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("100Gi")},
			}}},
		}))
		tooLarge := corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("101Gi")},
		}
		// Admission reads LimitRanges through its own cache.
		require.Eventually(t, func() bool {
			probe := original.DeepCopy()
			probe.ObjectMeta = metav1.ObjectMeta{Namespace: namespace, Name: "limit-range-probe"}
			probe.Spec.InitContainers[0].Resources = tooLarge
			return apierrors.IsForbidden(c.Create(ctx, probe, client.DryRunAll))
		}, 10*time.Second, 100*time.Millisecond, "the LimitRange never took effect")

		setSidecarResources(t, shard, tooLarge)
		for range 3 {
			_, err := r.Reconcile(ctx, requestOf(shard))
			require.NoError(t, err)
		}
		after := livePod(t, podKey)
		require.NotNil(t, after, "the only pod was deleted for a pod the LimitRange rejects")
		assert.Equal(t, original.UID, after.UID)
		cond := refusal(t, shard)
		require.NotNil(t, cond)
		assert.Contains(t, cond.Message, "maximum memory usage per Container is 100Gi")
		assert.Contains(t, cond.Message, `pods "`+podKey.Name+`" is forbidden`)
		assert.Equal(t, multigresv1alpha1.PhaseDegraded, shard.Status.Phase)
		require.Len(t, rejections(recorder), 1)
		requireQuiet(t, shard, &steady, func() ctrl.Result {
			result, err := r.Reconcile(ctx, requestOf(shard))
			require.NoError(t, err)
			return result
		})

		// A limit inside the range is accepted and replaces the pod.
		setSidecarResources(t, shard, valid)
		_, _ = r.Reconcile(ctx, requestOf(shard))
		if replaced := livePod(t, podKey); replaced != nil {
			assert.NotEqual(t, original.UID, replaced.UID)
		}
		assert.Nil(t, refusal(t, shard))
	})

	// A ResourceQuota answers 403 for a namespace sized for exactly its pods,
	// because the probe is counted next to the pod it replaces. That answer
	// does not say whether the replacement fits once the pod is gone, and
	// the pod is the shard's only one, so it is kept, with its data, and the
	// refusal is recorded. The replacement goes ahead once a dry run fits:
	// the namespace needs room for one more pod than it holds.
	t.Run("lite pod in a namespace at its pod quota", func(t *testing.T) {
		const namespace = "pod-quota"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		}))
		passwordSecret(t, namespace)
		shard := storedShardIn(t, namespace, "counted", true)
		recorder := record.NewFakeRecorder(500)
		r := &ShardReconciler{
			Client: c, APIReader: c, Scheme: scheme, Recorder: recorder,
			CreateTopoStore: newMemoryTopoFactory(),
		}
		podKey := types.NamespacedName{
			Namespace: namespace, Name: BuildPoolPodName(shard, "default", "zone1", 0),
		}
		_, err := r.Reconcile(ctx, requestOf(shard))
		original := livePod(t, podKey)
		require.NotNil(t, original, "the first reconcile must create the Lite pod: %v", err)
		dataClaim := client.ObjectKey{
			Namespace: namespace, Name: BuildPoolDataPVCName(shard, "default", "zone1", 0),
		}
		require.NoError(t, c.Get(ctx, dataClaim, &corev1.PersistentVolumeClaim{}))

		one := corev1.ResourceList{corev1.ResourcePods: resource.MustParse("1")}
		quota := fullQuota(t, namespace, "one-pod", one, original)

		setSidecarResources(t, shard, valid)
		for range 3 {
			_, err := r.Reconcile(ctx, requestOf(shard))
			require.NoError(t, err)
			kept := livePod(t, podKey)
			require.NotNil(t, kept, "the only pod was deleted on a quota's answer to a dry run")
			require.Equal(t, original.UID, kept.UID)
		}
		require.NoError(t, c.Get(ctx, dataClaim, &corev1.PersistentVolumeClaim{}))
		cond := refusal(t, shard)
		require.NotNil(t, cond, "a pod kept for a full quota must be reported")
		assert.Contains(t, cond.Message, "exceeded quota: one-pod")
		assert.Len(t, rejections(recorder), 1)
		assert.False(t, r.quotaWaiting.waiting(client.ObjectKeyFromObject(shard)),
			"no create was turned down")

		// The administrator makes room for one more pod, and the next pass
		// replaces the pod.
		two := corev1.ResourceList{corev1.ResourcePods: resource.MustParse("2")}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(quota), quota))
		quota.Spec.Hard = two
		require.NoError(t, c.Update(ctx, quota))
		quota.Status.Hard = two
		require.NoError(t, c.Status().Update(ctx, quota))
		require.Eventually(t, func() bool {
			_, _ = r.Reconcile(ctx, requestOf(shard))
			pod := livePod(t, podKey)
			return pod != nil && pod.UID != original.UID
		}, 10*time.Second, 200*time.Millisecond, "the pod was never replaced")
		_, _ = r.Reconcile(ctx, requestOf(shard))
		assert.Nil(t, refusal(t, shard))
	})

	// s3Lite is a Lite shard in its own namespace whose backups go to S3,
	// reconciled against one topology that lasts for the test, so that the
	// database record can be compared with the pod. Minigres reads the
	// backup location from that record each time it starts and refuses a
	// sidecar that was started with another, how the endpoint's certificate
	// is verified included.
	type s3Lite struct {
		shard    *multigresv1alpha1.Shard
		r        *ShardReconciler
		recorder *record.FakeRecorder
		podKey   types.NamespacedName
		original *corev1.Pod
		// recordedCA is the CA file in the database record's S3 location.
		recordedCA func() string
		// setCA edits the shard's S3 location to name ca.
		setCA func(ca *multigresv1alpha1.S3CAReference)
		// requireAgreement fails the test when the pod's sidecar and the
		// record disagree about the CA file.
		requireAgreement func(when string)
	}
	s3LiteIn := func(t *testing.T, namespace string) *s3Lite {
		t.Helper()
		require.NoError(t, c.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		}))
		passwordSecret(t, namespace)
		location := func(ca *multigresv1alpha1.S3CAReference) *multigresv1alpha1.BackupConfig {
			return &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeS3,
				S3: &multigresv1alpha1.S3BackupConfig{
					Bucket: "b", Region: "r", CredentialsSecret: "s3-creds",
					Endpoint: "minio.storage.svc:9000", CA: ca,
				},
			}
		}
		f := &s3Lite{recorder: record.NewFakeRecorder(500)}
		f.shard = storedShardIn(t, namespace, "located", true,
			func(shard *multigresv1alpha1.Shard) {
				shard.Spec.Backup = location(nil)
				shard.Spec.SidecarBackupLocation = location(nil)
			})
		_, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
		t.Cleanup(func() { _ = store.Close() })
		f.r = &ShardReconciler{
			Client: c, APIReader: c, Scheme: scheme, Recorder: f.recorder,
			CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
				return keepOpenTopo{store}, nil
			},
		}
		f.podKey = types.NamespacedName{
			Namespace: namespace, Name: BuildPoolPodName(f.shard, "default", "zone1", 0),
		}
		f.recordedCA = func() string {
			t.Helper()
			db, err := store.GetDatabase(ctx, string(f.shard.Spec.DatabaseName))
			require.NoError(t, err)
			require.NotNil(t, db.GetBackupLocation().GetS3())
			return db.GetBackupLocation().GetS3().GetCaFile()
		}
		f.setCA = func(ca *multigresv1alpha1.S3CAReference) {
			t.Helper()
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(f.shard), f.shard))
			f.shard.Spec.Backup = location(ca)
			f.shard.Spec.SidecarBackupLocation = location(ca)
			require.NoError(t, c.Update(ctx, f.shard))
		}
		f.requireAgreement = func(when string) {
			t.Helper()
			pod := livePod(t, f.podKey)
			if pod == nil {
				return
			}
			started := ""
			for _, container := range pod.Spec.InitContainers {
				for _, arg := range container.Args {
					if after, ok := strings.CutPrefix(arg, "--s3-ca-file="); ok &&
						container.Name == PgBackRestSidecarContainerName {
						started = after
					}
				}
			}
			require.Equal(t, started, f.recordedCA(),
				"%s: the record names another CA file than the pod's sidecar was started with, "+
					"so minigres would not start again in this pod", when)
		}
		_, err := f.r.Reconcile(ctx, requestOf(f.shard))
		f.original = livePod(t, f.podKey)
		require.NotNil(t, f.original, "the first reconcile must create the Lite pod: %v", err)
		require.Empty(t, f.recordedCA())
		f.requireAgreement("after the first reconcile")
		return f
	}
	// A bundle with one certificate block. Only its framing is read here.
	const caBundle = "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIUQ0FCdW5kbGVGb3JUZXN0c09ubHk=\n-----END CERTIFICATE-----\n"

	// The location of a Lite shard changes (here: a CA for the endpoint)
	// while its namespace has no room for the dry run, so the only pod is
	// kept. The database record must keep the location that pod was started
	// with for as long as it exists, and move to the new one when its
	// successor is created.
	t.Run("lite pod kept at a full quota keeps its location in topology", func(t *testing.T) {
		f := s3LiteIn(t, "lite-location")
		require.NoError(t, c.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "minio-ca", Namespace: f.podKey.Namespace},
			Data:       map[string]string{"ca.crt": caBundle},
		}))
		one := corev1.ResourceList{corev1.ResourcePods: resource.MustParse("1")}
		quota := fullQuota(t, f.podKey.Namespace, "one-pod", one, f.original)

		f.setCA(&multigresv1alpha1.S3CAReference{
			ConfigMap: &multigresv1alpha1.S3CAKeyReference{Name: "minio-ca"},
		})
		for range 3 {
			_, err := f.r.Reconcile(ctx, requestOf(f.shard))
			require.NoError(t, err)
			kept := livePod(t, f.podKey)
			require.NotNil(t, kept)
			require.Equal(t, f.original.UID, kept.UID, "the only pod was replaced at a full quota")
			require.Empty(t, f.recordedCA(),
				"the record moved to the new location while the pod runs on the old one")
			f.requireAgreement("while the pod is kept")
		}
		cond := refusal(t, f.shard)
		require.NotNil(t, cond)
		assert.Contains(t, cond.Message, "exceeded quota: one-pod")

		two := corev1.ResourceList{corev1.ResourcePods: resource.MustParse("2")}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(quota), quota))
		quota.Spec.Hard = two
		require.NoError(t, c.Update(ctx, quota))
		quota.Status.Hard = two
		require.NoError(t, c.Status().Update(ctx, quota))
		require.Eventually(t, func() bool {
			_, _ = f.r.Reconcile(ctx, requestOf(f.shard))
			f.requireAgreement("during the replacement")
			pod := livePod(t, f.podKey)
			return pod != nil && pod.UID != f.original.UID
		}, 10*time.Second, 200*time.Millisecond, "the pod was never replaced")
		require.Equal(t, multigresv1alpha1.S3CAFilePath, f.recordedCA())
		f.requireAgreement("after the replacement")
	})

	// The API server accepts a pod whose CA volume names an object that does
	// not exist, a key the object lacks, or a key without a certificate: the
	// kubelet or the sidecar finds out, after the pod it replaced is gone.
	// The Lite pod is the shard's only one, so it is kept, the refusal is
	// recorded, and it is replaced once the bundle is usable.
	t.Run(
		"lite pod is kept while the CA bundle of its replacement is unusable",
		func(t *testing.T) {
			f := s3LiteIn(t, "lite-ca")
			kept := func(when string) {
				t.Helper()
				for range 2 {
					_, err := f.r.Reconcile(ctx, requestOf(f.shard))
					require.NoError(t, err)
					pod := livePod(t, f.podKey)
					require.NotNil(t, pod, when)
					require.Equal(t, f.original.UID, pod.UID, "%s: the only pod was replaced", when)
					f.requireAgreement(when)
				}
			}
			f.setCA(&multigresv1alpha1.S3CAReference{
				Secret: &multigresv1alpha1.S3CAKeyReference{Name: "corp-ca"},
			})
			kept("the Secret does not exist")
			cond := refusal(t, f.shard)
			require.NotNil(t, cond, "a pod kept for an unusable CA bundle must be reported")
			assert.Equal(t, multigresv1alpha1.ReasonPodCABundleUnusable, cond.Reason)
			assert.Contains(t, cond.Message, `Secret "corp-ca" does not exist`)
			assert.Equal(t, multigresv1alpha1.PhaseDegraded, f.shard.Status.Phase)
			assert.Len(t, rejections(f.recorder), 1)

			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "corp-ca", Namespace: f.podKey.Namespace},
				Data:       map[string][]byte{"tls.crt": []byte(caBundle)},
			}
			require.NoError(t, c.Create(ctx, secret))
			kept("the Secret has no key ca.crt")
			secret.Data = map[string][]byte{"ca.crt": []byte("not a certificate")}
			require.NoError(t, c.Update(ctx, secret))
			kept("the key holds no certificate")
			assert.Empty(t, rejections(f.recorder), "a refusal that stands is reported once")

			secret.Data = map[string][]byte{"ca.crt": []byte(caBundle)}
			require.NoError(t, c.Update(ctx, secret))
			require.Eventually(t, func() bool {
				_, _ = f.r.Reconcile(ctx, requestOf(f.shard))
				f.requireAgreement("during the replacement")
				pod := livePod(t, f.podKey)
				return pod != nil && pod.UID != f.original.UID
			}, 10*time.Second, 200*time.Millisecond, "the pod was never replaced")
			_, _ = f.r.Reconcile(ctx, requestOf(f.shard))
			assert.Nil(t, refusal(t, f.shard))
			require.Equal(t, multigresv1alpha1.S3CAFilePath, f.recordedCA())
		},
	)

	// liteShardIn creates a namespace with a Lite shard whose pod exists, and
	// returns what a pass over it does.
	type liteCase struct {
		shard    *multigresv1alpha1.Shard
		recorder *record.FakeRecorder
		podKey   types.NamespacedName
		original *corev1.Pod
		pass     func() ctrl.Result
		// reconcile is pass for a pass that may fail.
		reconcile func() (ctrl.Result, error)
		// later is how far the reconciler's clock runs ahead of the test's.
		later time.Duration
		// waitingOnQuota is whether the shard has a create on record that
		// a ResourceQuota turned down.
		waitingOnQuota func() bool
		// wokenBy is the Shards an event about obj reconciles for a refusal.
		wokenBy func(obj client.Object) []reconcile.Request
	}
	liteShardIn := func(
		t *testing.T, namespace string, with ...func(*multigresv1alpha1.Shard),
	) *liteCase {
		t.Helper()
		require.NoError(t, c.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		}))
		passwordSecret(t, namespace)
		lc := &liteCase{
			shard:    storedShardIn(t, namespace, "shard", true, with...),
			recorder: record.NewFakeRecorder(500),
		}
		r := &ShardReconciler{
			Client: c, APIReader: c, Scheme: scheme, Recorder: lc.recorder,
			CreateTopoStore: newMemoryTopoFactory(),
			Clock:           func() time.Time { return time.Now().Add(lc.later) },
		}
		lc.podKey = types.NamespacedName{
			Namespace: namespace, Name: BuildPoolPodName(lc.shard, "default", "zone1", 0),
		}
		lc.reconcile = func() (ctrl.Result, error) {
			return r.Reconcile(ctx, requestOf(lc.shard))
		}
		lc.wokenBy = func(obj client.Object) []reconcile.Request {
			return r.enqueueRefusedShards(ctx, obj)
		}
		lc.waitingOnQuota = func() bool {
			return r.quotaWaiting.waiting(client.ObjectKeyFromObject(lc.shard))
		}
		lc.pass = func() ctrl.Result {
			result, err := lc.reconcile()
			require.NoError(t, err)
			return result
		}
		lc.pass()
		lc.original = livePod(t, lc.podKey)
		require.NotNil(t, lc.original, "the first reconcile must create the Lite pod")
		return lc
	}
	// podWebhook installs a validating webhook for pod creates in namespace
	// and waits until the API server uses it: a dry-run create must get the
	// answer want recognises.
	podWebhook := func(
		t *testing.T, lc *liteCase, name string,
		clientConfig admissionregistrationv1.WebhookClientConfig, want func(error) bool,
	) *admissionregistrationv1.ValidatingWebhookConfiguration {
		t.Helper()
		config := &admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Webhooks: []admissionregistrationv1.ValidatingWebhook{{
				Name:                    name + ".example.com",
				ClientConfig:            clientConfig,
				AdmissionReviewVersions: []string{"v1"},
				SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
				FailurePolicy:           ptr.To(admissionregistrationv1.Fail),
				TimeoutSeconds:          ptr.To(int32(2)),
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					"kubernetes.io/metadata.name": lc.podKey.Namespace,
				}},
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{
						admissionregistrationv1.Create,
					},
					Rule: admissionregistrationv1.Rule{
						APIGroups: []string{""}, APIVersions: []string{"v1"},
						Resources: []string{"pods"},
					},
				}},
			}},
		}
		require.NoError(t, c.Create(ctx, config))
		t.Cleanup(func() { _ = c.Delete(ctx, config) })
		require.Eventually(t, func() bool {
			probe := lc.original.DeepCopy()
			probe.ObjectMeta = metav1.ObjectMeta{Namespace: lc.podKey.Namespace, Name: "probe"}
			return want(c.Create(ctx, probe, client.DryRunAll))
		}, 15*time.Second, 100*time.Millisecond, "the webhook never took effect")
		return config
	}
	// removed deletes a webhook configuration and waits until a dry-run
	// create under the pod's own name is answered by storage again.
	removed := func(
		t *testing.T, lc *liteCase,
		config *admissionregistrationv1.ValidatingWebhookConfiguration,
	) {
		t.Helper()
		require.NoError(t, c.Delete(ctx, config))
		require.Eventually(t, func() bool {
			probe := lc.original.DeepCopy()
			probe.ObjectMeta = metav1.ObjectMeta{
				Namespace: lc.podKey.Namespace, Name: lc.podKey.Name,
			}
			return apierrors.IsAlreadyExists(c.Create(ctx, probe, client.DryRunAll))
		}, 15*time.Second, 100*time.Millisecond, "the webhook never went away")
	}
	replaced := func(t *testing.T, lc *liteCase) bool {
		t.Helper()
		pod := livePod(t, lc.podKey)
		return pod == nil || pod.UID != lc.original.UID
	}

	// A webhook that the API server cannot reach, with failurePolicy Fail,
	// makes every pod create a 500. That is not a judgement of the pod, and
	// the Lite pod is the shard's only one: it is neither deleted on the
	// strength of it, nor does it undo a rejection already on record.
	t.Run("lite pod while the API server gives no answer", func(t *testing.T) {
		lc := liteShardIn(t, "unanswered")
		unreachable := admissionregistrationv1.WebhookClientConfig{
			URL: ptr.To("https://127.0.0.1:1/validate"),
		}
		serverError := func(err error) bool { return apierrors.IsInternalError(err) }

		// A rejection on record first.
		setSidecarResources(t, lc.shard, rejectedSidecarResources[2].resources)
		for range 3 {
			lc.pass()
		}
		recorded := refusal(t, lc.shard)
		require.NotNil(t, recorded)

		down := podWebhook(t, lc, "unreachable", unreachable, serverError)
		for range 3 {
			lc.pass()
		}
		require.False(t, replaced(t, lc), "the only pod was deleted on a server error")
		after := refusal(t, lc.shard)
		require.NotNil(t, after, "a server error removed the recorded rejection")
		assert.Equal(t, recorded.Message, after.Message)
		assert.Equal(t, multigresv1alpha1.PhaseDegraded, lc.shard.Status.Phase)

		// The same with a template that is fine: the pod still waits for an
		// answer, and nothing is reported as rejected.
		setSidecarResources(t, lc.shard, valid)
		for range 3 {
			lc.pass()
		}
		require.False(t, replaced(t, lc), "the only pod was deleted on a server error")
		assert.Nil(t, refusal(t, lc.shard))
		assert.NotEmpty(t, eventsOf(lc.recorder, "PodReplacementUnanswered"),
			"a pod kept for want of an answer must be reported")
		waiting := lc.pass()
		requireQuiet(t, lc.shard, &waiting, lc.pass)

		// The API server answers again, and the shard asks again once its
		// wait since the last unanswered dry run is over.
		removed(t, lc, down)
		lc.later += time.Hour
		lc.pass()
		require.True(t, replaced(t, lc), "an accepted replacement must go ahead")
	})

	// An admission webhook may deny a create with any status, 409 included.
	// Only the storage layer's AlreadyExists for the pod's own name means
	// the pod passed admission.
	t.Run("lite pod denied by a webhook with 409", func(t *testing.T) {
		lc := liteShardIn(t, "reserved")
		var dryRuns atomic.Int32
		server := httptest.NewTLSServer(http.HandlerFunc(
			func(w http.ResponseWriter, req *http.Request) {
				review := &admissionv1.AdmissionReview{}
				if err := json.NewDecoder(req.Body).Decode(review); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if review.Request.DryRun != nil && *review.Request.DryRun {
					dryRuns.Add(1)
				}
				review.Response = &admissionv1.AdmissionResponse{
					UID: review.Request.UID,
					Result: &metav1.Status{
						Status: metav1.StatusFailure, Code: http.StatusConflict,
						Reason:  metav1.StatusReasonConflict,
						Message: "external reservation is occupied",
					},
				}
				review.Request = nil
				_ = json.NewEncoder(w).Encode(review)
			}))
		defer server.Close()
		deny := podWebhook(t, lc, "reservations", admissionregistrationv1.WebhookClientConfig{
			URL: ptr.To(server.URL + "/validate"),
			CABundle: pem.EncodeToMemory(&pem.Block{
				Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
			}),
		}, func(err error) bool {
			return apierrors.IsConflict(err) && !apierrors.IsAlreadyExists(err)
		})

		setSidecarResources(t, lc.shard, valid)
		steady := lc.pass()
		for range 3 {
			lc.pass()
		}
		require.False(t, replaced(t, lc), "the only pod was deleted for a pod a webhook denies")
		require.Positive(t, dryRuns.Load(), "the webhook must have been asked by dry run")
		cond := refusal(t, lc.shard)
		require.NotNil(t, cond)
		assert.Contains(t, cond.Message, "external reservation is occupied")
		assert.Equal(t, multigresv1alpha1.PhaseDegraded, lc.shard.Status.Phase)
		requireQuiet(t, lc.shard, &steady, lc.pass)

		// The reservation is released: no spec edit is needed.
		removed(t, lc, deny)
		lc.pass()
		require.True(t, replaced(t, lc))
		lc.pass()
		assert.Nil(t, refusal(t, lc.shard))
	})

	// A replacement that asks for more memory than the namespace's quota has
	// left is the same answer to the dry run: the Lite pod is kept and the
	// refusal recorded, and the pod is replaced on the first pass after the
	// namespace has room for the replacement next to it.
	t.Run("lite pod whose replacement outgrows a memory quota", func(t *testing.T) {
		// The pod's memory request is its sidecar's, from the defaults a
		// cluster resolves into the pool.
		lc := liteShardIn(t, "shared-quota", func(shard *multigresv1alpha1.Shard) {
			pool := shard.Spec.Pools["default"]
			pool.PgBackRestResources = resolver.DefaultResourcesPgBackRest()
			shard.Spec.Pools["default"] = pool
		})
		own := podresources.PodRequests(lc.original, podresources.PodResourcesOptions{})
		ownMemory := own[corev1.ResourceMemory]
		require.False(t, ownMemory.IsZero(), "the fixture's pod must request memory")
		// A quota on memory requests admits only pods whose containers all
		// state one, so the namespace supplies a small default.
		require.NoError(t, c.Create(ctx, &corev1.LimitRange{
			ObjectMeta: metav1.ObjectMeta{Name: "defaults", Namespace: lc.podKey.Namespace},
			Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
				Type: corev1.LimitTypeContainer,
				DefaultRequest: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("1Mi"),
				},
			}}},
		}))
		// Other workloads use 100Gi, and the quota is exactly full.
		others := resource.MustParse("100Gi")
		full := others.DeepCopy()
		full.Add(ownMemory)
		hard := corev1.ResourceList{corev1.ResourceRequestsMemory: full}
		quota := fullQuota(t, lc.podKey.Namespace, "memory", hard, lc.original)

		setSidecarResources(t, lc.shard, corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("2304Mi"),
		}})
		for range 3 {
			lc.pass()
			require.False(t, replaced(t, lc),
				"the only pod was deleted on a quota's answer to a dry run")
		}
		cond := refusal(t, lc.shard)
		require.NotNil(t, cond, "a pod kept for a full quota must be reported")
		assert.Contains(t, cond.Message, "exceeded quota: memory")
		assert.Len(t, eventsOf(lc.recorder, "PodTemplateRejected"), 1)
		assert.False(t, lc.waitingOnQuota(), "no create was turned down")

		// The administrator makes room.
		roomy := full.DeepCopy()
		roomy.Add(resource.MustParse("4Gi"))
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(quota), quota))
		quota.Spec.Hard = corev1.ResourceList{corev1.ResourceRequestsMemory: roomy}
		require.NoError(t, c.Update(ctx, quota))
		quota.Status.Hard = quota.Spec.Hard
		require.NoError(t, c.Status().Update(ctx, quota))
		require.Eventually(t, func() bool {
			_, err := lc.reconcile()
			pod := livePod(t, lc.podKey)
			return err == nil && pod != nil && pod.UID != lc.original.UID
		}, 10*time.Second, 200*time.Millisecond, "the replacement was never created")
		lc.pass()
		assert.Nil(t, refusal(t, lc.shard))
	})

	// The shared backup claim is created by a server-side apply, which a
	// quota on claims or storage turns down like any create. The pass ends
	// there, and the Shard is on record as waiting on the quota as it is for
	// a pod or a data claim. A pass within the claim's wait sends no apply
	// and ends the same way, and the first pass after the wait creates the
	// claim once the quota has room, with nothing reading or watching the
	// quota.
	for _, lite := range []bool{false, true} {
		name := "distributed"
		if lite {
			name = "lite"
		}
		t.Run("shared backup claim over quota, "+name, func(t *testing.T) {
			namespace := "backup-claim-" + name
			require.NoError(t, c.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespace},
			}))
			passwordSecret(t, namespace)
			none := corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("0"),
			}
			quota := &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "claims", Namespace: namespace},
				Spec:       corev1.ResourceQuotaSpec{Hard: none},
			}
			require.NoError(t, c.Create(ctx, quota))
			// No quota controller runs here: the usage is written by hand,
			// and the API server's admission does the rest.
			quota.Status = corev1.ResourceQuotaStatus{Hard: none, Used: none}
			require.NoError(t, c.Status().Update(ctx, quota))

			shard := storedShardIn(t, namespace, "shard", lite)
			claim, err := BuildSharedBackupPVC(shard, true, scheme)
			require.NoError(t, err)
			require.NotNil(t, claim)
			applies := 0
			var later time.Duration
			r := &ShardReconciler{
				APIReader: c, Scheme: scheme, Recorder: record.NewFakeRecorder(500),
				CreateTopoStore: newMemoryTopoFactory(),
				Clock:           func() time.Time { return time.Now().Add(later) },
				Client: interceptor.NewClient(c, interceptor.Funcs{
					Patch: func(
						ctx context.Context, c client.WithWatch, obj client.Object,
						patch client.Patch, opts ...client.PatchOption,
					) error {
						if obj.GetName() == claim.Name && patch.Type() == types.ApplyPatchType {
							applies++
						}
						return c.Patch(ctx, obj, patch, opts...)
					},
				}),
			}
			key := client.ObjectKeyFromObject(shard)
			request := requestOf(shard)
			// Admission reads quotas through its own cache.
			require.Eventually(t, func() bool {
				_, err := r.Reconcile(ctx, request)
				return quotaExceeded(err)
			}, 10*time.Second, 100*time.Millisecond)
			require.True(t, r.quotaWaiting.waiting(key), "the Shard must wait on the quota")

			sent := applies
			_, err = r.Reconcile(ctx, request)
			require.ErrorContains(t, err, "failed to apply shared backup PVC")
			require.True(t, quotaExceeded(err))
			require.Equal(t, sent, applies, "an apply was sent within the claim's wait")
			later += quotaHoldCap
			_, err = r.Reconcile(ctx, request)
			require.ErrorContains(t, err, "failed to apply shared backup PVC")
			require.True(t, quotaExceeded(err))
			require.Equal(t, sent+1, applies, "no apply was sent after the claim's wait")
			pods := &corev1.PodList{}
			require.NoError(t, c.List(ctx, pods, client.InNamespace(namespace)))
			require.Empty(t, pods.Items, "a pod was created without the claim it mounts")

			// The administrator makes room, and the next pass creates the
			// claim.
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(quota), quota))
			room := corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("10"),
			}
			quota.Spec.Hard = room
			require.NoError(t, c.Update(ctx, quota))
			quota.Status.Hard = room
			require.NoError(t, c.Status().Update(ctx, quota))
			later += quotaHoldCap
			require.Eventually(t, func() bool {
				_, _ = r.Reconcile(ctx, request)
				return c.Get(ctx, client.ObjectKeyFromObject(claim),
					&corev1.PersistentVolumeClaim{}) == nil
			}, 20*time.Second, 200*time.Millisecond, "the claim was never created")

			// An apply that would change nothing is not sent, so another
			// writer first changes a label the operator applies.
			live := &corev1.PersistentVolumeClaim{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(claim), live))
			live.Labels[metadata.LabelAppManagedBy] = "someone-else"
			require.NoError(t, c.Update(ctx, live))
			sent = applies
			require.NoError(t, r.reconcileSharedBackupPVC(ctx, shard))
			assert.Equal(t, sent+1, applies)
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(claim), live))
			assert.Equal(t, metadata.ManagedByMultigres, live.Labels[metadata.LabelAppManagedBy])
			// The repaired claim is what the spec asks for, and nothing is
			// sent for it.
			require.NoError(t, r.reconcileSharedBackupPVC(ctx, shard))
			assert.Equal(t, sent+1, applies)
		})
	}

	// A failed create of a desired pod does not end the pass while the shard
	// holds a pod beyond a replica count, so that scale-down can go on. When
	// the rest of that pass succeeds and asks for no requeue, the pass still
	// fails with the create's error: nothing else would bring the shard back
	// to try the create again.
	t.Run("a create failure carried to the end of the pass fails it", func(t *testing.T) {
		const namespace = "carried"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		}))
		passwordSecret(t, namespace)
		shard := storedShardIn(t, namespace, "shard", false)
		pool := shard.Spec.Pools["default"]
		pool.ReplicasPerCell = ptr.To(int32(2))
		shard.Spec.Pools["default"] = pool
		require.NoError(t, c.Update(ctx, shard))
		// Pod 0 stays, and is not Ready on an API server without a kubelet,
		// so scale-down does not start and no requeue is asked for on its
		// account.
		desired := types.NamespacedName{
			Namespace: namespace, Name: BuildPoolPodName(shard, "default", "zone1", 1),
		}
		refused := apierrors.NewInternalError(errors.New("admission webhook unreachable"))
		refusing := false
		r := &ShardReconciler{
			APIReader: c, Scheme: scheme, Recorder: record.NewFakeRecorder(500),
			CreateTopoStore: newMemoryTopoFactory(),
			Client: interceptor.NewClient(c, interceptor.Funcs{
				Create: func(
					ctx context.Context, c client.WithWatch, obj client.Object,
					opts ...client.CreateOption,
				) error {
					if pod, isPod := obj.(*corev1.Pod); isPod && refusing &&
						pod.Name == desired.Name {
						return refused
					}
					return c.Create(ctx, obj, opts...)
				},
			}),
		}
		_, err := r.Reconcile(ctx, requestOf(shard))
		require.NoError(t, err)
		require.NotNil(t, livePod(t, desired), "the first reconcile must create the pod")

		// A pod beyond the replica count, and the desired pod gone.
		extra, err := BuildPoolPod(shard, "default", "zone1", pool, 2, scheme)
		require.NoError(t, err)
		require.NoError(t, c.Create(ctx, extra))
		require.NoError(t, c.Delete(ctx, livePod(t, desired), client.GracePeriodSeconds(0)))
		require.Eventually(t, func() bool { return livePod(t, desired) == nil },
			10*time.Second, 100*time.Millisecond)
		refusing = true

		result, err := r.Reconcile(ctx, requestOf(shard))
		require.ErrorIs(t, err, refused, "result %+v", result)
		require.ErrorContains(t, err, "failed to create pod "+desired.Name)
		assert.Nil(t, livePod(t, desired))
		assert.NotNil(t, livePod(t, client.ObjectKeyFromObject(extra)))
	})

	// A refusal can end without a spec edit: its cause goes away, and the pod
	// is deleted or created again by someone else. The pass after that asks
	// about no replacement, so the status pass has to notice, in the status
	// the API server stores.
	t.Run("lite refusal ends with its cause", func(t *testing.T) {
		lc := liteShardIn(t, "refusal-ends")
		limit := &corev1.LimitRange{
			ObjectMeta: metav1.ObjectMeta{Name: "max", Namespace: lc.podKey.Namespace},
			Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
				Type: corev1.LimitTypeContainer,
				Max:  corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("100Gi")},
			}}},
		}
		tooLarge := corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("101Gi")},
		}
		refused := func(t *testing.T) {
			t.Helper()
			require.NoError(t, c.Create(ctx, limit.DeepCopy()))
			// Admission reads LimitRanges through its own cache.
			require.Eventually(t, func() bool {
				probe := lc.original.DeepCopy()
				probe.ObjectMeta = metav1.ObjectMeta{Namespace: lc.podKey.Namespace, Name: "probe"}
				probe.Spec.InitContainers[0].Resources = tooLarge
				return apierrors.IsForbidden(c.Create(ctx, probe, client.DryRunAll))
			}, 10*time.Second, 100*time.Millisecond, "the LimitRange never took effect")
			setSidecarResources(t, lc.shard, tooLarge)
			for range 3 {
				lc.pass()
			}
			require.NotNil(t, refusal(t, lc.shard))
			require.Equal(t, multigresv1alpha1.PhaseDegraded, lc.shard.Status.Phase)
			// Nothing of the Shard's changes when the LimitRange does, so
			// what brings it back is the LimitRange's own event, and a
			// requeue no later than the recheck for causes that have no
			// event.
			assert.Empty(t, lc.wokenBy(&corev1.LimitRange{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "elsewhere"},
			}))
			assert.Equal(t, []reconcile.Request{requestOf(lc.shard)}, lc.wokenBy(limit))
			held := lc.pass()
			assert.Positive(t, held.RequeueAfter)
			assert.LessOrEqual(t, held.RequeueAfter,
				refusalRecheckInterval+refusalRecheckInterval/5)
		}
		limitGone := func(t *testing.T) {
			t.Helper()
			require.NoError(t, c.Delete(ctx, limit.DeepCopy()))
			require.Eventually(t, func() bool {
				probe := lastProbe[lc.podKey.Namespace].DeepCopy()
				probe.Name = "probe"
				return direct.Create(ctx, probe, client.DryRunAll) == nil
			}, 10*time.Second, 100*time.Millisecond, "the LimitRange never went away")
		}
		cleared := func(t *testing.T) {
			t.Helper()
			lc.pass()
			assert.Nil(t, refusal(t, lc.shard), "the refusal outlived its cause")
			assert.NotEqual(t, multigresv1alpha1.PhaseDegraded, lc.shard.Status.Phase)
			assert.NotContains(t, lc.shard.Status.Message, "API server rejects")
			generation := lc.shard.Generation
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(lc.shard), lc.shard))
			require.Equal(t, generation, lc.shard.Generation, "no spec edit is involved")
		}

		// The pod is created again on the current spec by someone else, so
		// no pass has a replacement to ask about.
		refused(t)
		limitGone(t)
		pod := livePod(t, lc.podKey)
		require.NotNil(t, pod)
		require.NoError(t, c.Delete(ctx, pod, client.GracePeriodSeconds(0)))
		current := lastProbe[lc.podKey.Namespace].DeepCopy()
		require.NoError(t, c.Create(ctx, current))
		cleared(t)
		lc.original = livePod(t, lc.podKey)
		require.NotNil(t, lc.original, "a pod on the current spec was replaced")
		require.Equal(t, current.UID, lc.original.UID)

		// The pod is deleted and nothing else creates it.
		setSidecarResources(t, lc.shard, valid)
		lc.pass()
		lc.pass()
		lc.original = livePod(t, lc.podKey)
		require.NotNil(t, lc.original)
		refused(t)
		limitGone(t)
		require.NoError(t, c.Delete(ctx, lc.original, client.GracePeriodSeconds(0)))
		cleared(t)
	})

	// count/pods counts Pod objects whatever their phase. A failed Lite pod
	// in a namespace at its count/pods limit holds the one object the quota
	// has room for, and deleting it is what makes room for its replacement.
	t.Run("failed lite pod at a count/pods quota", func(t *testing.T) {
		lc := liteShardIn(t, "object-count")
		failed := lc.original.DeepCopy()
		failed.Status.Phase = corev1.PodFailed
		failed.Status.Reason = "Evicted"
		require.NoError(t, c.Status().Update(ctx, failed))
		one := corev1.ResourceList{"count/pods": resource.MustParse("1")}
		quota := fullQuota(t, lc.podKey.Namespace, "objects", one, lc.original)

		lc.pass()
		require.Nil(t, livePod(t, lc.podKey),
			"a failed pod was kept for a quota that its own deletion frees")
		assert.Nil(t, refusal(t, lc.shard))
		releaseQuota(t, quota, corev1.ResourceList{"count/pods": resource.MustParse("0")})
		require.Eventually(t, func() bool {
			lc.pass()
			return livePod(t, lc.podKey) != nil
		}, 10*time.Second, 200*time.Millisecond, "the replacement was never created")
		assert.NotEqual(t, lc.original.UID, livePod(t, lc.podKey).UID)
	})

	// An API server refuses every dry run that reaches a webhook declaring
	// side effects it cannot suppress, while it lets the create through. No
	// later pass could ask either, so the pod is replaced as it would be
	// without the question. admissionregistration.k8s.io/v1 does not store
	// such a declaration (only configurations written through v1beta1 carry
	// it), so this webhook gives the API server's answer itself, to dry runs
	// only.
	t.Run("lite pod behind a webhook that cannot answer a dry run", func(t *testing.T) {
		lc := liteShardIn(t, "no-dry-run")
		server := httptest.NewTLSServer(http.HandlerFunc(
			func(w http.ResponseWriter, req *http.Request) {
				review := &admissionv1.AdmissionReview{}
				if err := json.NewDecoder(req.Body).Decode(review); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				review.Response = &admissionv1.AdmissionResponse{
					UID: review.Request.UID, Allowed: true,
				}
				if review.Request.DryRun != nil && *review.Request.DryRun {
					review.Response.Allowed = false
					review.Response.Result = &metav1.Status{
						Status: metav1.StatusFailure, Code: http.StatusBadRequest,
						Reason:  metav1.StatusReasonBadRequest,
						Message: `admission webhook "legacy.example.com" does not support dry run`,
					}
				}
				review.Request = nil
				_ = json.NewEncoder(w).Encode(review)
			}))
		defer server.Close()
		podWebhook(t, lc, "legacy", admissionregistrationv1.WebhookClientConfig{
			URL: ptr.To(server.URL + "/validate"),
			CABundle: pem.EncodeToMemory(&pem.Block{
				Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
			}),
		}, func(err error) bool {
			return apierrors.IsBadRequest(err) &&
				strings.Contains(err.Error(), "does not support dry run")
		})

		setSidecarResources(t, lc.shard, valid)
		lc.pass()
		require.True(t, replaced(t, lc), "a question that cannot be asked held the rollout")
		require.Eventually(t, func() bool {
			lc.pass()
			return livePod(t, lc.podKey) != nil
		}, 10*time.Second, 200*time.Millisecond, "the replacement was never created")
		assert.Nil(t, refusal(t, lc.shard))
		assert.Empty(t, eventsOf(lc.recorder, "PodReplacementUnanswered"))
	})

	// The operator builds its child resources by server-side apply. Applying
	// an empty object over a pgbackrestResources block that had entries is
	// rejected by the API server, so a block cleared by leaving an empty map
	// behind must reach the apply as no block at all.
	t.Run("cleared block can be applied", func(t *testing.T) {
		stored := storedShard(t, "cleared-resources", false)
		apply := func(res corev1.ResourceRequirements, normalize bool) error {
			shard := &multigresv1alpha1.Shard{
				TypeMeta: metav1.TypeMeta{
					APIVersion: multigresv1alpha1.GroupVersion.String(), Kind: "Shard",
				},
				ObjectMeta: metav1.ObjectMeta{Name: stored.Name, Namespace: stored.Namespace},
				Spec:       *stored.Spec.DeepCopy(),
			}
			pool := shard.Spec.Pools["default"]
			pool.PgBackRestResources = res
			if normalize {
				multigresv1alpha1.NormalizeResources(&pool.PgBackRestResources)
			}
			shard.Spec.Pools["default"] = pool
			return c.Patch(ctx, shard, client.Apply,
				client.FieldOwner("multigres-operator"), client.ForceOwnership)
		}
		for name, cleared := range map[string]corev1.ResourceRequirements{
			"no block":       {},
			"empty requests": {Requests: corev1.ResourceList{}},
			"empty limits":   {Limits: corev1.ResourceList{}},
			"empty claims":   {Claims: []corev1.ResourceClaim{}},
		} {
			require.NoError(t, apply(valid, true), name)
			require.NoError(t, apply(cleared, true), name)
			got := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(stored), got))
			assert.Empty(t, got.Spec.Pools["default"].PgBackRestResources.Requests, name)
			assert.Empty(t, got.Spec.Pools["default"].PgBackRestResources.Limits, name)
		}
		// What the normalising prevents.
		require.NoError(t, apply(valid, true))
		err := apply(corev1.ResourceRequirements{Requests: corev1.ResourceList{}}, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be of type object")
	})
}
