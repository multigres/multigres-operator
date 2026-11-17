package multigrescluster

import (
	"context"
	"errors"
	"fmt"
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
			{name: "legacy retained PVC with explicit root", mutate: func(c *multigresv1alpha1.MultigresCluster) { c.Spec.Backup.S3.KeyPrefix = "/" }, wantPrefix: "/", objects: []client.Object{&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-shared-name-global-topo-0", Namespace: "default", Labels: map[string]string{metadata.LabelMultigresCluster: "shared-name"}}}}},
			{name: "fresh recovery from explicit root", mutate: func(c *multigresv1alpha1.MultigresCluster) { c.Spec.Backup.S3.KeyPrefix = "/" }, wantPrefix: "/", wantDefault: true},
			{name: "unmounted claim with matching cluster label", objects: []client.Object{&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "monitoring-history", Namespace: "default", Labels: map[string]string{metadata.LabelMultigresCluster: "shared-name"}}}}, wantPrefix: "clusters/uid-a", wantDefault: true},
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

func TestBackupPrefixWithResourcesFromPreviousCluster(t *testing.T) {
	clusterOwner := metav1.OwnerReference{
		APIVersion: multigresv1alpha1.GroupVersion.String(), Kind: "MultigresCluster",
		Name: "shared-name", UID: "old-uid",
	}
	currentOwner := clusterOwner
	currentOwner.UID = "uid-a"
	shardOwner := clusterOwner
	shardOwner.Kind = "Shard"
	otherGroup := clusterOwner
	otherGroup.APIVersion = "example.com/v1"
	for _, tc := range []struct {
		name       string
		deleting   bool
		owners     []metav1.OwnerReference
		retained   bool
		wantPrefix string
	}{
		{name: "terminating", deleting: true, wantPrefix: "clusters/uid-a"},
		{name: "previous cluster", owners: []metav1.OwnerReference{clusterOwner}, wantPrefix: "clusters/uid-a"},
		{name: "current cluster", owners: []metav1.OwnerReference{currentOwner}},
		{name: "both clusters", owners: []metav1.OwnerReference{clusterOwner, currentOwner}},
		{name: "indirect owner", owners: []metav1.OwnerReference{shardOwner}},
		{name: "different API group", owners: []metav1.OwnerReference{otherGroup}},
		{name: "terminating alongside retained PVC", deleting: true, retained: true},
	} {
		for _, obj := range []client.Object{&corev1.PersistentVolumeClaim{}, &multigresv1alpha1.TableGroup{}} {
			t.Run(fmt.Sprintf("%T/%s", obj, tc.name), func(t *testing.T) {
				cluster := backupPrefixCluster("")
				cluster.Spec.Backup = nil
				labels := map[string]string{metadata.LabelMultigresCluster: cluster.Name}
				obj.SetNamespace(cluster.Namespace)
				obj.SetName("data-shared-name-global-topo-0")
				obj.SetLabels(labels)
				obj.SetOwnerReferences(tc.owners)
				if tc.deleting {
					now := metav1.Now()
					obj.SetFinalizers([]string{"test.multigres.com/hold"})
					obj.SetDeletionTimestamp(&now)
				}
				objects := []client.Object{cluster, obj}
				if tc.retained {
					objects = append(
						objects,
						&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
							Namespace: cluster.Namespace,
							Name:      "data-shared-name-global-topo-1",
							Labels:    labels,
						}},
					)
				}
				c := fake.NewClientBuilder().
					WithScheme(setupScheme()).
					WithObjects(objects...).
					Build()
				r := &MultigresClusterReconciler{Client: c}
				require.NoError(t, r.ensureClusterFinalizer(t.Context(), cluster))
				require.Equal(
					t,
					tc.wantPrefix,
					cluster.Annotations[multigresv1alpha1.AnnotationDefaultBackupKeyPrefix],
				)
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
				Name: "data-shared-name-global-topo-0", Namespace: cluster.Namespace,
				Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
			},
		}).
		Build()
	r := &MultigresClusterReconciler{Client: cached, APIReader: live}
	require.ErrorContains(
		t,
		r.ensureClusterFinalizer(t.Context(), cluster),
		"set the original S3 keyPrefix",
	)
	require.Empty(t, cluster.Finalizers)
	require.Empty(t, cluster.Annotations[multigresv1alpha1.AnnotationDefaultBackupKeyPrefix])
	require.Empty(t, cluster.Spec.Backup.S3.KeyPrefix)
}

func TestConflictingRetainedAllocationsStopInitialization(t *testing.T) {
	cluster := backupPrefixCluster("")
	key := multigresv1alpha1.AnnotationDefaultBackupKeyPrefix
	objects := []client.Object{cluster}
	for i, prefix := range []string{"clusters/old-a", "clusters/old-b"} {
		objects = append(objects, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf(
				"data-%s-global-topo-%d",
				cluster.Name,
				i,
			),
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				metadata.LabelMultigresCluster: cluster.Name,
			},
			Annotations: map[string]string{key: prefix},
		}})
	}
	c := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
	r := &MultigresClusterReconciler{Client: c}
	require.ErrorContains(
		t,
		r.ensureClusterFinalizer(t.Context(), cluster),
		"conflicting backup allocations",
	)
	require.Empty(t, cluster.Finalizers)
	require.Empty(t, cluster.Annotations)
}
