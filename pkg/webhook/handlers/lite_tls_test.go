package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestLiteRejectsPublicTLSOnCreateAndUpdate(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(&storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "standard",
			Annotations: map[string]string{"storageclass.kubernetes.io/is-default-class": "true"},
		},
		Provisioner: "example.com/storage",
	}).Build()
	validator := NewMultigresClusterValidator(c)
	for _, mode := range []string{multigresv1alpha1.ModeLite, ""} {
		for _, operation := range []string{"create", "update"} {
			t.Run(string(mode)+"/"+operation, func(t *testing.T) {
				cluster := &multigresv1alpha1.MultigresCluster{
					ObjectMeta: metav1.ObjectMeta{Name: "tls-test", Namespace: "default"},
					Spec: multigresv1alpha1.MultigresClusterSpec{
						Mode:           mode,
						CertCommonName: "db.example.com",
						Cells:          []multigresv1alpha1.CellConfig{{Name: "z1"}},
						Databases: []multigresv1alpha1.DatabaseConfig{
							{
								Name:    "postgres",
								Default: true,
								TableGroups: []multigresv1alpha1.TableGroupConfig{
									{
										Name:    "default",
										Default: true,
										Shards: []multigresv1alpha1.ShardConfig{
											{
												Name: "0-inf",
												Spec: &multigresv1alpha1.ShardInlineSpec{
													Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
														"main": {Type: "readWrite"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}
				var err error
				if operation == "create" {
					_, err = validator.ValidateCreate(t.Context(), cluster)
				} else {
					old := cluster.DeepCopy()
					old.Spec.CertCommonName = ""
					_, err = validator.ValidateUpdate(t.Context(), old, cluster)
				}
				if mode == multigresv1alpha1.ModeLite {
					require.ErrorContains(t, err, "lite mode does not support certCommonName")
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}
