package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

func TestBackupDefaultAdmission(t *testing.T) {
	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		t.Run(mode, func(t *testing.T) {
			cluster := identityCluster()
			cluster.Spec.Mode = mode
			cluster.Spec.Backup = &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeS3,
				S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "shared", Region: "us-east-1"},
			}
			original := identityTemplate("original", "main")
			pool := original.Spec.Pools["main"]
			pool.Cells = []multigresv1alpha1.CellName{"z1"}
			original.Spec.Pools["main"] = pool
			c := identityClient(original)
			d := NewMultigresClusterDefaulter(resolver.NewResolver(c, cluster.Namespace))
			v := NewMultigresClusterValidator(c)
			// Admission has no cluster UID. Creation accepts the omitted prefix;
			// the controller allocates it before provisioning anything.
			require.NoError(t, d.Default(t.Context(), cluster))
			_, err := v.ValidateCreate(t.Context(), cluster)
			require.NoError(t, err)
			require.Empty(t, cluster.Spec.Backup.S3.KeyPrefix)
			cluster.UID = "cluster-uid"
			old := cluster.DeepCopy()
			cluster.Annotations[multigresv1alpha1.AnnotationDefaultBackupKeyPrefix] = "clusters/cluster-uid"
			cluster.Finalizers = []string{multigresv1alpha1.FinalizerClusterCleanup}
			require.NoError(t, d.Default(t.Context(), cluster))
			_, err = v.ValidateUpdate(t.Context(), old, cluster)
			require.NoError(t, err)
			require.Equal(t, "clusters/cluster-uid", cluster.Spec.Backup.S3.KeyPrefix)
			// Reapplying a manifest without the default uses the same allocation.
			old = cluster.DeepCopy()
			cluster.Spec.Backup.S3.KeyPrefix = ""
			require.NoError(t, d.Default(t.Context(), cluster))
			_, err = v.ValidateUpdate(t.Context(), old, cluster)
			require.NoError(t, err)
			require.Equal(t, old.Spec.Backup, cluster.Spec.Backup)
		})
	}
}

func TestBackupDefaultAnnotationValidation(t *testing.T) {
	key := multigresv1alpha1.AnnotationDefaultBackupKeyPrefix
	for _, tc := range []struct {
		name, old, new         string
		initialized, wantError bool
	}{
		{"initial allocation", "", "clusters/uid", false, false},
		{"wrong identity", "", "clusters/other", false, true},
		{"stable", "clusters/uid", "clusters/uid", true, false},
		{"removal", "clusters/uid", "", true, true},
		{"replacement", "clusters/uid", "clusters/other", true, true},
		{"legacy", "", "", true, false},
		{"legacy cannot opt in", "", "clusters/uid", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := identityCluster()
			old.UID = "uid"
			old.Annotations = map[string]string{key: tc.old}
			if tc.initialized {
				old.Finalizers = []string{multigresv1alpha1.FinalizerClusterCleanup}
			}
			updated := old.DeepCopy()
			updated.Annotations[key] = tc.new
			err := validateBackupDefaultUpdate(old, updated)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
	cluster := identityCluster()
	cluster.Annotations = map[string]string{key: "clusters/copied-uid"}
	_, err := NewMultigresClusterValidator(identityClient()).ValidateCreate(t.Context(), cluster)
	require.ErrorContains(t, err, "omit it when creating")
}
