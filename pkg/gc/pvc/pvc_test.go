package pvc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/multigres/multigres-operator/pkg/gc"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

const (
	tsOld    = "2026-01-01T00-00-00Z" // way past retention
	tsRecent = "2026-05-10T00-00-00Z" // inside retention
	tsNow    = "2026-05-15T00-00-00Z"
)

func pvc(name string, lbls map[string]string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns1", Labels: lbls},
	}
}

func orphanLabels(ts string) map[string]string {
	return map[string]string{
		metadata.LabelAppManagedBy: metadata.ManagedByMultigres,
		metadata.LabelOrphan:       ts,
	}
}

// run builds a fake client + Cleankeeper and invokes Clean. Returns Results.
func run(
	t *testing.T,
	opts gc.Options,
	interceptors interceptor.Funcs,
	objs ...client.Object,
) (*gc.Results, client.Client) {
	t.Helper()
	scheme := clientgoscheme.Scheme
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptors).
		Build()
	if opts.Now == nil {
		opts.Now = parseNow(t, tsNow)
	}
	if opts.Retention == 0 {
		opts.Retention = 30 * 24 * time.Hour
	}
	res, err := New(cl, testr.New(t), opts).Clean(context.Background())
	require.NoError(t, err)
	return res, cl
}

func parseNow(t *testing.T, s string) func() time.Time {
	t.Helper()
	ts, err := time.Parse(metadata.OrphanTimestampFormat, s)
	require.NoError(t, err)
	return func() time.Time { return ts }
}

func TestClean_DeletesExpiredOnly(t *testing.T) {
	t.Parallel()

	res, cl := run(
		t,
		gc.Options{},
		interceptor.Funcs{},
		pvc("old", orphanLabels(tsOld)),       // eligible
		pvc("recent", orphanLabels(tsRecent)), // inside window
		pvc("unmanaged", map[string]string{metadata.LabelOrphan: tsOld}),
		pvc(
			"unlabeled",
			map[string]string{metadata.LabelAppManagedBy: metadata.ManagedByMultigres},
		),
	)

	require.Equal(t, ObjectKind, res.Kind)
	require.Equal(t, 2, res.Scanned)
	require.Equal(t, 1, res.Deleted)
	require.Equal(t, 1, res.Skipped)
	require.Zero(t, res.Errors+res.Malformed+res.WouldDelete)

	require.True(t, apierrors.IsNotFound(
		cl.Get(
			context.Background(),
			client.ObjectKey{Namespace: "ns1", Name: "old"},
			&corev1.PersistentVolumeClaim{},
		),
	))
}

func TestClean_DryRun(t *testing.T) {
	t.Parallel()

	res, _ := run(t, gc.Options{DryRun: true}, interceptor.Funcs{},
		pvc("old", orphanLabels(tsOld)),
	)

	require.Equal(t, 1, res.WouldDelete)
	require.Zero(t, res.Deleted)
}

func TestClean_MalformedTimestamp(t *testing.T) {
	t.Parallel()

	res, _ := run(t, gc.Options{}, interceptor.Funcs{},
		pvc("bad", orphanLabels("not-a-timestamp")),
	)

	require.Equal(t, 1, res.Malformed)
	require.Zero(t, res.Deleted)
}

func TestClean_NamespaceScope(t *testing.T) {
	t.Parallel()

	a := pvc("a", orphanLabels(tsOld))
	b := pvc("b", orphanLabels(tsOld))
	b.Namespace = "ns2"

	res, _ := run(t, gc.Options{Namespace: "ns1"}, interceptor.Funcs{}, a, b)

	require.Equal(t, 1, res.Scanned)
	require.Equal(t, 1, res.Deleted)
}

func TestClean_DeleteErrorCounted(t *testing.T) {
	t.Parallel()

	boom := errors.New("api server unavailable")
	res, _ := run(t, gc.Options{}, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return boom
		},
	}, pvc("old", orphanLabels(tsOld)))

	require.Equal(t, 1, res.Errors)
	require.Zero(t, res.Deleted)
}

func TestClean_NotFoundIsNotError(t *testing.T) {
	t.Parallel()

	res, _ := run(t, gc.Options{}, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return apierrors.NewNotFound(corev1.Resource("persistentvolumeclaims"), obj.GetName())
		},
	}, pvc("old", orphanLabels(tsOld)))

	require.Zero(t, res.Errors)
}

// Pinpoint regression: a PVC orphaned later in the day must NOT be deleted
// until a full retention has elapsed from that exact timestamp.
func TestClean_SameDayPrecision(t *testing.T) {
	t.Parallel()

	res, _ := run(t, gc.Options{
		Retention: 30 * 24 * time.Hour,
		Now:       parseNow(t, "2026-05-15T12-00-00Z"),
	}, interceptor.Funcs{}, pvc("borderline", orphanLabels("2026-04-15T18-00-00Z")))

	require.Equal(t, 1, res.Skipped)
	require.Zero(t, res.Deleted)
}

func podUsing(ns, name, claim string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
			},
		}}},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// countDeletes is an interceptor that records every Delete it passes through.
func countDeletes(n *int) interceptor.Funcs {
	return interceptor.Funcs{
		Delete: func(
			ctx context.Context,
			c client.WithWatch,
			obj client.Object,
			opts ...client.DeleteOption,
		) error {
			*n++
			return c.Delete(ctx, obj, opts...)
		},
	}
}

func pvcExists(t *testing.T, cl client.Client, ns, name string) *corev1.PersistentVolumeClaim {
	t.Helper()
	got := &corev1.PersistentVolumeClaim{}
	require.NoError(
		t,
		cl.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, got),
	)
	return got
}

// An orphan label on a PVC a pod references is stale: deleting the PVC would
// only leave it Terminating under the pod, to be recreated empty once the pod
// goes. pvc-protection counts a pod object in any phase, so this does too.
func TestClean_LeavesPVCUsedByAPod(t *testing.T) {
	t.Parallel()

	for _, phase := range []corev1.PodPhase{
		corev1.PodPending, corev1.PodRunning, corev1.PodUnknown, "",
		corev1.PodSucceeded, corev1.PodFailed,
	} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			deletes := 0
			res, cl := run(t, gc.Options{}, countDeletes(&deletes),
				pvc("old", orphanLabels(tsOld)),
				podUsing("ns1", "pooler-0", "old", phase),
			)

			require.Equal(t, 1, res.InUse)
			require.Zero(t, res.Deleted)
			require.Zero(t, deletes)
			require.Nil(t, pvcExists(t, cl, "ns1", "old").DeletionTimestamp)
		})
	}
}

func TestClean_DryRunReportsInUse(t *testing.T) {
	t.Parallel()

	res, _ := run(t, gc.Options{DryRun: true}, interceptor.Funcs{},
		pvc("old", orphanLabels(tsOld)),
		podUsing("ns1", "pooler-0", "old", corev1.PodRunning),
	)

	require.Equal(t, 1, res.InUse)
	require.Zero(t, res.WouldDelete)
}

// A pod in another namespace naming a claim with the same name, or a pod
// using a different claim, does not hold the PVC.
func TestClean_DeletesPVCWithoutUser(t *testing.T) {
	t.Parallel()

	res, cl := run(t, gc.Options{}, interceptor.Funcs{},
		pvc("old", orphanLabels(tsOld)),
		podUsing("ns2", "elsewhere", "old", corev1.PodRunning),
		podUsing("ns1", "unrelated", "other", corev1.PodRunning),
	)

	require.Equal(t, 1, res.Deleted)
	require.Zero(t, res.InUse)
	require.True(t, apierrors.IsNotFound(cl.Get(
		context.Background(),
		client.ObjectKey{Namespace: "ns1", Name: "old"},
		&corev1.PersistentVolumeClaim{},
	)))
}

func TestClean_SkipsPVCAlreadyBeingDeleted(t *testing.T) {
	t.Parallel()

	terminating := pvc("old", orphanLabels(tsOld))
	terminating.Finalizers = []string{"kubernetes.io/pvc-protection"}
	terminating.DeletionTimestamp = &metav1.Time{Time: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)}

	deletes := 0
	res, _ := run(t, gc.Options{}, countDeletes(&deletes), terminating)

	require.Equal(t, 1, res.Skipped)
	require.Zero(t, res.Deleted+res.Errors)
	require.Zero(t, deletes)
}

// Without the pod list the in-use check cannot be answered, so the run stops
// before deleting anything.
func TestClean_PodListErrorAbortsWithoutDeleting(t *testing.T) {
	t.Parallel()

	deletes := 0
	funcs := countDeletes(&deletes)
	funcs.List = func(
		ctx context.Context,
		c client.WithWatch,
		list client.ObjectList,
		opts ...client.ListOption,
	) error {
		if _, ok := list.(*corev1.PodList); ok {
			return errors.New("pods are forbidden")
		}
		return c.List(ctx, list, opts...)
	}
	cl := fake.NewClientBuilder().
		WithScheme(clientgoscheme.Scheme).
		WithObjects(pvc("old", orphanLabels(tsOld))).
		WithInterceptorFuncs(funcs).
		Build()

	_, err := New(cl, testr.New(t), gc.Options{
		Now:       parseNow(t, tsNow),
		Retention: 30 * 24 * time.Hour,
	}).Clean(context.Background())

	require.ErrorContains(t, err, "pods are forbidden")
	require.Zero(t, deletes)
	pvcExists(t, cl, "ns1", "old")
}

// The operator reclaiming a PVC between this run's List and its Delete clears
// the label, which bumps the resourceVersion the Delete is conditioned on.
func TestClean_ReclaimBetweenListAndDeleteWins(t *testing.T) {
	t.Parallel()

	res, cl := run(t, gc.Options{}, interceptor.Funcs{
		Delete: func(
			ctx context.Context,
			c client.WithWatch,
			obj client.Object,
			opts ...client.DeleteOption,
		) error {
			live := &corev1.PersistentVolumeClaim{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), live); err != nil {
				return err
			}
			if err := pvcutil.ClearOrphan(ctx, logr.Discard(), c, live); err != nil {
				return err
			}
			return c.Delete(ctx, obj, opts...)
		},
	}, pvc("old", orphanLabels(tsOld)))

	require.Equal(t, 1, res.Errors)
	require.Zero(t, res.Deleted)
	got := pvcExists(t, cl, "ns1", "old")
	require.Nil(t, got.DeletionTimestamp)
	require.False(t, pvcutil.HasOrphanLabel(got))
}

// Static check that *Cleankeeper satisfies gc.Cleankeeper.
var _ gc.Cleankeeper = (*Cleankeeper)(nil)
