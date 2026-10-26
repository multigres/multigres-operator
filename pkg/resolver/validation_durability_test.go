package resolver

import (
	"slices"
	"strings"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolver_ValidateClusterLogic_MultiPoolDurability(t *testing.T) {
	t.Parallel()

	pool := func(cells ...multigresv1alpha1.CellName) multigresv1alpha1.PoolSpec {
		return multigresv1alpha1.PoolSpec{Cells: cells, ReplicasPerCell: ptr.To(int32(1))}
	}
	tests := map[string]struct {
		policy         string
		databasePolicy string
		pools          map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec
		wantWarnings   []string
	}{
		"default policy across three single-member pools": {
			pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"a": pool("zone-1"), "b": pool("zone-2"), "c": pool("zone-3"),
			},
		},
		"AT_LEAST_2 across pools in the same cell": {
			policy: "AT_LEAST_2",
			pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"a": pool("zone-1"), "b": pool("zone-1"),
			},
		},
		"MULTI_CELL_AT_LEAST_2 across three single-member pools": {
			policy: "MULTI_CELL_AT_LEAST_2",
			pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"a": pool("zone-1"), "b": pool("zone-2"), "c": pool("zone-3"),
			},
		},
		"MULTI_CELL_AT_LEAST_2 across two single-member pools": {
			policy: "MULTI_CELL_AT_LEAST_2",
			pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"a": pool("zone-1"), "b": pool("zone-2"),
			},
		},
		"database AT_LEAST_2 permits pools in the same cell": {
			policy:         "MULTI_CELL_AT_LEAST_2",
			databasePolicy: "AT_LEAST_2",
			pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"a": pool("zone-1"), "b": pool("zone-1"),
			},
		},
		"database MULTI_CELL_AT_LEAST_2 warns for pools in the same cell": {
			policy:         "AT_LEAST_2",
			databasePolicy: "MULTI_CELL_AT_LEAST_2",
			pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"a": pool("zone-1"), "b": pool("zone-1"),
			},
			wantWarnings: []string{
				"shard '0-80' spans 1 cell(s); durability policy \"MULTI_CELL_AT_LEAST_2\" requires at least 2 distinct cells.",
			},
		},
		"MULTI_CELL_AT_LEAST_2 across overlapping pools": {
			policy: "MULTI_CELL_AT_LEAST_2",
			pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"a": pool("zone-1", "zone-2"), "b": pool("zone-2"),
			},
		},
		"MULTI_CELL_AT_LEAST_2 counts a shared cell only once": {
			policy: "MULTI_CELL_AT_LEAST_2",
			pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"a": pool("zone-1"), "b": pool("zone-1"), "c": pool("zone-1"),
			},
			wantWarnings: []string{
				"shard '0-80' spans 1 cell(s); durability policy \"MULTI_CELL_AT_LEAST_2\" requires at least 2 distinct cells.",
			},
		},
		"MULTI_CELL_AT_LEAST_2 resolves omitted cells and replicas": {
			policy: "MULTI_CELL_AT_LEAST_2",
			pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"a": {}, "b": pool("zone-1"),
			},
		},
		"AT_LEAST_2 still warns for one member": {
			policy: "AT_LEAST_2",
			pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"a": pool("zone-1"),
			},
			wantWarnings: []string{
				"shard '0-80' has 1 total pooler(s) across 1 cell(s); durability policy \"AT_LEAST_2\" requires at least 2 total poolers.",
			},
		},
	}

	for name, tc := range tests {
		for _, source := range []string{"inline", "template", "override"} {
			t.Run(name+"/"+source, func(t *testing.T) {
				t.Parallel()
				shard := multigresv1alpha1.ShardConfig{
					Name: "0-80",
					Spec: &multigresv1alpha1.ShardInlineSpec{Pools: tc.pools},
				}
				template := &multigresv1alpha1.ShardTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "durability", Namespace: "default"},
					Spec:       multigresv1alpha1.ShardTemplateSpec{Pools: tc.pools},
				}
				if source != "inline" {
					shard.Spec = nil
					shard.ShardTemplate = "durability"
				}
				if source == "override" {
					// The effective pools must come from overrides, not the template's
					// otherwise sufficient three-cell placement.
					template.Spec.Pools = make(
						map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec,
					)
					for name := range tc.pools {
						template.Spec.Pools[name] = pool("zone-1", "zone-2", "zone-3")
					}
					shard.Overrides = &multigresv1alpha1.ShardOverrides{Pools: tc.pools}
				}
				cluster := durabilityTestCluster(shard)
				cluster.Spec.DurabilityPolicy = tc.policy
				cluster.Spec.Databases[0].DurabilityPolicy = tc.databasePolicy
				c := fake.NewClientBuilder().WithScheme(setupScheme()).
					WithObjects(template, durabilityTestStorageClass()).Build()
				r := NewResolver(c, "default")
				warnings, err := r.ValidateClusterLogic(t.Context(), cluster)
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(warnings, tc.wantWarnings) {
					t.Fatalf("warnings = %v, want %v", warnings, tc.wantWarnings)
				}
			})
		}
	}
}

func TestResolver_ValidateClusterLogic_DurabilityShardIsolation(t *testing.T) {
	t.Parallel()

	for _, policy := range []string{"AT_LEAST_2", "MULTI_CELL_AT_LEAST_2"} {
		for _, boundary := range []string{"shard", "tablegroup", "database"} {
			t.Run(policy+"/"+boundary, func(t *testing.T) {
				t.Parallel()
				shard := func(cell multigresv1alpha1.CellName) multigresv1alpha1.ShardConfig {
					return multigresv1alpha1.ShardConfig{
						Name: "0-80",
						Spec: &multigresv1alpha1.ShardInlineSpec{
							Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
								"a": {
									Cells:           []multigresv1alpha1.CellName{cell},
									ReplicasPerCell: ptr.To(int32(1)),
								},
							},
						},
					}
				}
				cluster := durabilityTestCluster(shard("zone-1"))
				second := shard("zone-2")
				db := &cluster.Spec.Databases[0]
				// Exercise the database override rather than the cluster policy.
				cluster.Spec.DurabilityPolicy = "MULTI_CELL_AT_LEAST_2"
				db.DurabilityPolicy = policy
				switch boundary {
				case "shard":
					second.Name = "80-inf"
					db.TableGroups[0].Shards = append(db.TableGroups[0].Shards, second)
				case "tablegroup":
					db.TableGroups = append(db.TableGroups, multigresv1alpha1.TableGroupConfig{
						Name: "other", Shards: []multigresv1alpha1.ShardConfig{second},
					})
				case "database":
					cluster.Spec.Databases = append(
						cluster.Spec.Databases,
						multigresv1alpha1.DatabaseConfig{
							Name: "other", DurabilityPolicy: policy,
							TableGroups: []multigresv1alpha1.TableGroupConfig{{
								Name: "commerce", Shards: []multigresv1alpha1.ShardConfig{second},
							}},
						},
					)
				}
				c := fake.NewClientBuilder().WithScheme(setupScheme()).
					WithObjects(durabilityTestStorageClass()).Build()
				warnings, err := NewResolver(
					c,
					"default",
				).ValidateClusterLogic(t.Context(), cluster)
				if err != nil {
					t.Fatal(err)
				}
				if len(warnings) != 2 {
					t.Fatalf("warnings = %v, want one per undersized shard", warnings)
				}
				for _, warning := range warnings {
					if !strings.Contains(
						warning,
						"durability policy \""+policy+"\" requires at least 2",
					) {
						t.Errorf("unexpected warning: %s", warning)
					}
				}
			})
		}
	}
}

func durabilityTestCluster(
	shard multigresv1alpha1.ShardConfig,
) *multigresv1alpha1.MultigresCluster {
	return &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "durability", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Cells: []multigresv1alpha1.CellConfig{
				{Name: "zone-1"},
				{Name: "zone-2"},
				{Name: "zone-3"},
			},
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
					Storage: multigresv1alpha1.StorageSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
					},
				},
			},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name: "postgres",
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name: "commerce", Shards: []multigresv1alpha1.ShardConfig{shard},
				}},
			}},
		},
	}
}

func durabilityTestStorageClass() *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "standard",
			Annotations: map[string]string{"storageclass.kubernetes.io/is-default-class": "true"},
		},
		Provisioner: "k8s.io/fake",
	}
}
