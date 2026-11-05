package pvc

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	shardUID types.UID = "shard-uid-1234"
	otherUID types.UID = "other-uid-9999"
)

func newPVC(name string, ownerUIDs ...types.UID) *corev1.PersistentVolumeClaim {
	refs := make([]metav1.OwnerReference, 0, len(ownerUIDs))
	for _, uid := range ownerUIDs {
		refs = append(refs, metav1.OwnerReference{UID: uid})
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       "ns1",
			OwnerReferences: refs,
		},
	}
}

func newClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(clientgoscheme.Scheme).
		WithObjects(objs...).
		Build()
}

func TestMarkOrphan_AddsLabelAndStripsOwnerRef(t *testing.T) {
	t.Parallel()

	pvc := newPVC("a", shardUID, otherUID)
	c := newClient(t, pvc)
	now := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)

	require.NoError(t, MarkOrphan(context.Background(), c, pvc, shardUID, now))

	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(pvc), got))
	require.Equal(t, "2026-05-19T12-00-00Z", got.Labels[metadata.LabelOrphan])
	require.Len(t, got.OwnerReferences, 1)
	require.Equal(t, otherUID, got.OwnerReferences[0].UID)
}

func TestMarkOrphan_Idempotent(t *testing.T) {
	t.Parallel()

	pvc := newPVC("a")
	pvc.Labels = map[string]string{metadata.LabelOrphan: "2026-05-01T00-00-00Z"}
	c := newClient(t, pvc)
	now := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)

	require.NoError(t, MarkOrphan(context.Background(), c, pvc, "", now))

	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(pvc), got))
	// Existing label preserved — retention is measured from the original event.
	require.Equal(t, "2026-05-01T00-00-00Z", got.Labels[metadata.LabelOrphan])
}

func TestMarkOrphan_NoOwnerUIDLeavesRefs(t *testing.T) {
	t.Parallel()

	pvc := newPVC("a", shardUID)
	c := newClient(t, pvc)

	require.NoError(t, MarkOrphan(context.Background(), c, pvc, "", time.Now()))

	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(pvc), got))
	require.Len(t, got.OwnerReferences, 1)
}

func TestClearOrphan_RemovesLabel(t *testing.T) {
	t.Parallel()

	logger := logr.Discard()
	pvc := newPVC("a")
	pvc.Labels = map[string]string{
		metadata.LabelOrphan:       "2026-05-01T00-00-00Z",
		metadata.LabelAppManagedBy: metadata.ManagedByMultigres,
	}
	c := newClient(t, pvc)

	require.NoError(t, ClearOrphan(context.Background(), logger, c, pvc))

	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(pvc), got))
	_, hasOrphan := got.Labels[metadata.LabelOrphan]
	require.False(t, hasOrphan)
	// other labels should remain untouched.
	require.Equal(t, metadata.ManagedByMultigres, got.Labels[metadata.LabelAppManagedBy])
}

func TestClearOrphan_NoLabelIsNoOp(t *testing.T) {
	t.Parallel()

	logger := logr.Discard()
	pvc := newPVC("a")
	pvc.Labels = map[string]string{metadata.LabelAppManagedBy: metadata.ManagedByMultigres}
	c := newClient(t, pvc)

	require.NoError(t, ClearOrphan(context.Background(), logger, c, pvc))

	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(pvc), got))
	require.Equal(t, metadata.ManagedByMultigres, got.Labels[metadata.LabelAppManagedBy])
}

func TestHasOrphanLabel(t *testing.T) {
	t.Parallel()

	require.False(t, HasOrphanLabel(nil))
	require.False(t, HasOrphanLabel(newPVC("a")))

	labeled := newPVC("a")
	labeled.Labels = map[string]string{metadata.LabelOrphan: "x"}
	require.True(t, HasOrphanLabel(labeled))
}

// pvcProtection is the finalizer the StorageObjectInUseProtection admission
// plugin puts on every PVC. The fake client runs no admission, so tests that
// need a deleted PVC to linger add it themselves.
const pvcProtection = "kubernetes.io/pvc-protection"

func orphanedPVC(name string) *corev1.PersistentVolumeClaim {
	pvc := newPVC(name)
	pvc.Finalizers = []string{pvcProtection}
	pvc.Labels = map[string]string{
		metadata.LabelOrphan:       "2026-05-01T00-00-00Z",
		metadata.LabelAppManagedBy: metadata.ManagedByMultigres,
	}
	return pvc
}

func TestClearOrphan_RefusesTerminatingPVC(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := newClient(t, orphanedPVC("a"))
	require.NoError(t, c.Delete(ctx, newPVC("a")))

	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "a"}, got))
	require.True(t, IsTerminating(got))

	err := ClearOrphan(ctx, logr.Discard(), c, got)
	require.ErrorIs(t, err, ErrTerminating)

	after := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(got), after))
	require.Equal(t, got.ResourceVersion, after.ResourceVersion, "a refused clear must not write")
}

// A GC deletion that commits between the reconciler's read and its clearing
// patch must fail the patch, so the reconciler never treats the PVC as
// reclaimed.
func TestClearOrphan_LosesToDeletionAfterRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := newClient(t, orphanedPVC("a"))

	read := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "a"}, read))
	require.NoError(t, c.Delete(ctx, newPVC("a"), client.Preconditions{
		ResourceVersion: &read.ResourceVersion,
	}))

	err := ClearOrphan(ctx, logr.Discard(), c, read)
	require.True(t, apierrors.IsConflict(err), "want Conflict, got %v", err)
	require.True(t, HasOrphanLabel(read), "a failed clear must leave the caller's copy as stored")

	stored := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(read), stored))
	require.True(t, IsTerminating(stored))
	require.True(t, HasOrphanLabel(stored))
}

// The reverse order: the clear commits first, so the deletion's
// resourceVersion precondition, taken before the clear, fails.
func TestClearOrphan_WinsAgainstDeletionWithStaleVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := newClient(t, orphanedPVC("a"))

	listed := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "ns1", Name: "a"}, listed))
	reclaim := listed.DeepCopy()
	require.NoError(t, ClearOrphan(ctx, logr.Discard(), c, reclaim))

	err := c.Delete(
		ctx,
		newPVC("a"),
		client.Preconditions{ResourceVersion: &listed.ResourceVersion},
	)
	require.True(t, apierrors.IsConflict(err), "want Conflict, got %v", err)

	stored := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(listed), stored))
	require.False(t, IsTerminating(stored))
	require.False(t, HasOrphanLabel(stored))
}
