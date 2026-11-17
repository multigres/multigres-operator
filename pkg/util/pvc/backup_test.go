package pvc

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestRecordBackupPrefix(t *testing.T) {
	ctx := t.Context()
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "test"},
	}
	c := fake.NewClientBuilder().WithObjects(claim).Build()
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(claim), claim))
	stale := claim.DeepCopy()
	const prefix = "clusters/first"
	require.NoError(t, RecordBackupPrefix(ctx, c, claim, prefix))
	confirmed := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(claim), confirmed))
	require.Equal(t, prefix, confirmed.Annotations[api.AnnotationDefaultBackupKeyPrefix])
	rv := confirmed.ResourceVersion
	require.NoError(t, RecordBackupPrefix(ctx, c, confirmed, prefix))
	require.Equal(t, rv, confirmed.ResourceVersion)
	require.ErrorContains(t, RecordBackupPrefix(ctx, c, confirmed, "clusters/next"), prefix)
	require.True(t, apierrors.IsConflict(RecordBackupPrefix(ctx, c, stale, "clusters/next")))
	require.Empty(t, stale.Annotations)
}
