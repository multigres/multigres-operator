package multigrescluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func backupPrefixCluster(mode string) *multigresv1alpha1.MultigresCluster {
	return &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-name", Namespace: "default", UID: "uid-a"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Mode: mode,
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeS3,
				S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "shared", Region: "us-east-1"},
			},
		},
	}
}

func TestInitializeBackupKeyPrefix(t *testing.T) {
	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		for _, tc := range []struct {
			name        string
			mutate      func(*multigresv1alpha1.MultigresCluster)
			objects     []client.Object
			wantPrefix  string
			wantDefault bool
		}{
			{name: "new", wantPrefix: "clusters/uid-a", wantDefault: true},
			{name: "explicit prefix", mutate: func(c *multigresv1alpha1.MultigresCluster) { c.Spec.Backup.S3.KeyPrefix = "original/" }, wantPrefix: "original/", wantDefault: true},
			{name: "slash only", mutate: func(c *multigresv1alpha1.MultigresCluster) { c.Spec.Backup.S3.KeyPrefix = "///" }, wantPrefix: "clusters/uid-a", wantDefault: true},
			{name: "legacy finalizer", mutate: func(c *multigresv1alpha1.MultigresCluster) {
				c.Finalizers = []string{multigresv1alpha1.FinalizerClusterCleanup}
			}},
			{name: "legacy status", mutate: func(c *multigresv1alpha1.MultigresCluster) { c.Status.ObservedGeneration = 1 }},
			{name: "existing tablegroup", objects: []client.Object{&multigresv1alpha1.TableGroup{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "default", Labels: map[string]string{metadata.LabelMultigresCluster: "shared-name"}}}}},
			{name: "retained PVC", objects: []client.Object{&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "old-data", Namespace: "default", Labels: map[string]string{metadata.LabelMultigresCluster: "shared-name"}}}}},
			{name: "unrelated PVC", objects: []client.Object{&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default", Labels: map[string]string{metadata.LabelMultigresCluster: "other"}}}}, wantPrefix: "clusters/uid-a", wantDefault: true},
			{name: "recreated name new UID", mutate: func(c *multigresv1alpha1.MultigresCluster) { c.UID = "uid-b" }, wantPrefix: "clusters/uid-b", wantDefault: true},
		} {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				cluster := backupPrefixCluster(mode)
				if tc.mutate != nil {
					tc.mutate(cluster)
				}
				objects := append(tc.objects, cluster)
				c := fake.NewClientBuilder().
					WithScheme(setupScheme()).
					WithObjects(objects...).
					Build()
				r := &MultigresClusterReconciler{Client: c}
				require.NoError(t, r.ensureClusterFinalizer(t.Context(), cluster))
				stored := &multigresv1alpha1.MultigresCluster{}
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), stored))
				require.Contains(t, stored.Finalizers, multigresv1alpha1.FinalizerClusterCleanup)
				require.Equal(t, tc.wantPrefix, stored.Spec.Backup.S3.KeyPrefix)
				_, allocated := stored.Annotations[multigresv1alpha1.AnnotationDefaultBackupKeyPrefix]
				require.Equal(t, tc.wantDefault, allocated)
				// Restart the controller and reapply defaults, as happens without admission.
				restarted := &MultigresClusterReconciler{Client: c}
				require.NoError(t, restarted.ensureClusterFinalizer(t.Context(), stored))
				_, err := resolver.NewResolver(c, stored.Namespace).
					PopulateClusterDefaults(t.Context(), stored)
				require.NoError(t, err)
				require.Equal(t, tc.wantPrefix, stored.Spec.Backup.S3.KeyPrefix)
			})
		}
	}
}

func TestBackupPrefixCommittedBeforeProvisioning(t *testing.T) {
	cluster := backupPrefixCluster(multigresv1alpha1.ModeLite)
	base := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(cluster).Build()
	c := interceptor.NewClient(
		base,
		interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return errors.New("write unavailable")
			},
		},
	)
	r := &MultigresClusterReconciler{Client: c}
	_, err := r.Reconcile(
		t.Context(),
		ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)},
	)
	require.ErrorContains(t, err, "write unavailable")
	groups := &multigresv1alpha1.TableGroupList{}
	require.NoError(t, base.List(t.Context(), groups))
	require.Empty(t, groups.Items)
	stored := &multigresv1alpha1.MultigresCluster{}
	require.NoError(
		t,
		base.Get(
			t.Context(),
			types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
			stored,
		),
	)
	require.Empty(t, stored.Finalizers)
	require.Empty(t, stored.Spec.Backup.S3.KeyPrefix)
	// A retry generates the same UID-based prefix and commits it with the finalizer.
	r.Client = base
	require.NoError(t, r.ensureClusterFinalizer(t.Context(), stored))
	require.Equal(t, "clusters/uid-a", stored.Spec.Backup.S3.KeyPrefix)
}

func TestBackupPrefixUsedWhenS3IsAddedLater(t *testing.T) {
	cluster := backupPrefixCluster("")
	cluster.Spec.Backup = nil
	c := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(cluster).Build()
	r := &MultigresClusterReconciler{Client: c}
	require.NoError(t, r.ensureClusterFinalizer(t.Context(), cluster))
	require.Nil(t, cluster.Spec.Backup, "allocating a prefix must not enable S3")
	cluster.Spec.Backup = backupPrefixCluster("").Spec.Backup
	require.NoError(t, c.Update(t.Context(), cluster))
	_, err := resolver.NewResolver(c, cluster.Namespace).
		PopulateClusterDefaults(t.Context(), cluster)
	require.NoError(t, err)
	require.Equal(t, "clusters/uid-a", cluster.Spec.Backup.S3.KeyPrefix)
}

func TestBackupPrefixInitializationReadFailures(t *testing.T) {
	for _, failPVC := range []bool{false, true} {
		cluster := backupPrefixCluster("")
		base := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(cluster).Build()
		reader := interceptor.NewClient(
			base,
			interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					_, isPVC := list.(*corev1.PersistentVolumeClaimList)
					if isPVC == failPVC {
						return errors.New("read unavailable")
					}
					return c.List(ctx, list, opts...)
				},
			},
		)
		r := &MultigresClusterReconciler{Client: base, APIReader: reader}
		require.ErrorContains(t, r.ensureClusterFinalizer(t.Context(), cluster), "read unavailable")
		stored := &multigresv1alpha1.MultigresCluster{}
		require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(cluster), stored))
		require.Empty(t, stored.Finalizers)
		require.Empty(t, stored.Annotations)
	}
}

func TestBackupPrefixRetainedPVCUsesUncachedReader(t *testing.T) {
	cluster := backupPrefixCluster("")
	cached := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(cluster).Build()
	live := fake.NewClientBuilder().
		WithScheme(setupScheme()).
		WithObjects(&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "retained", Namespace: cluster.Namespace,
				Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
			},
		}).
		Build()
	r := &MultigresClusterReconciler{Client: cached, APIReader: live}
	require.NoError(t, r.ensureClusterFinalizer(t.Context(), cluster))
	require.Empty(t, cluster.Annotations[multigresv1alpha1.AnnotationDefaultBackupKeyPrefix])
	require.Empty(t, cluster.Spec.Backup.S3.KeyPrefix)
}
