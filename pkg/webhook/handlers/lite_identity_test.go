package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func identityCluster() *multigresv1alpha1.MultigresCluster {
	return &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "lite", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Mode: multigresv1alpha1.ModeLite, Cells: []multigresv1alpha1.CellConfig{{Name: "z1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name: "postgres", Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{
					{
						Name:    "default",
						Default: true,
						Shards: []multigresv1alpha1.ShardConfig{
							{Name: "0-inf", ShardTemplate: "original"},
						},
					},
				},
			}},
		},
	}
}

func identityTemplate(name, pool string) *multigresv1alpha1.ShardTemplate {
	return &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: multigresv1alpha1.ShardTemplateSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				multigresv1alpha1.PoolName(pool): {Type: "readWrite"},
			},
		},
	}
}

func identityClient(objects ...client.Object) client.Client {
	objects = append(
		objects,
		&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "standard",
				Annotations: map[string]string{
					"storageclass.kubernetes.io/is-default-class": "true",
				},
			},
			Provisioner: "example.test/storage",
		},
	)
	return fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
}

func TestLiteIdentityAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*multigresv1alpha1.MultigresCluster)
		want   string
	}{
		{"explicit template switch", func(c *multigresv1alpha1.MultigresCluster) {
			c.Spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = "different"
		}, "identity is immutable"},
		{"default template switch", func(c *multigresv1alpha1.MultigresCluster) {
			c.Spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = ""
			c.Spec.TemplateDefaults.ShardTemplate = "different"
		}, "identity is immutable"},
		{"inline pool rename", func(c *multigresv1alpha1.MultigresCluster) {
			s := &c.Spec.Databases[0].TableGroups[0].Shards[0]
			s.ShardTemplate = ""
			s.Spec = &multigresv1alpha1.ShardInlineSpec{Pools: identityTemplate("unused", "other").Spec.Pools}
		}, "identity is immutable"},
		{"same identity different template", func(c *multigresv1alpha1.MultigresCluster) {
			c.Spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = "equivalent"
		}, ""},
		{"clear mode", func(c *multigresv1alpha1.MultigresCluster) { c.Spec.Mode = "" }, "mode is immutable"},
		{"extra cluster cell", func(c *multigresv1alpha1.MultigresCluster) {
			c.Spec.Cells = append(c.Spec.Cells, multigresv1alpha1.CellConfig{Name: "z2"})
		}, "cluster cell"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := identityTemplate("original", "main")
			pool := original.Spec.Pools["main"]
			pool.Cells = []multigresv1alpha1.CellName{"z1"}
			original.Spec.Pools["main"] = pool
			c := identityClient(
				original,
				identityTemplate("different", "other"),
				identityTemplate("equivalent", "main"),
			)
			validator := NewMultigresClusterValidator(c)
			old := identityCluster()
			updated := old.DeepCopy()
			tc.mutate(updated)
			_, err := validator.ValidateUpdate(t.Context(), old, updated)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
			if tc.name == "extra cluster cell" {
				_, err = validator.ValidateCreate(t.Context(), updated)
				require.ErrorContains(t, err, "exactly one cluster cell")
			}
		})
	}
	t.Run("enter lite", func(t *testing.T) {
		old := identityCluster()
		old.Spec.Mode = ""
		_, err := NewMultigresClusterValidator(
			identityClient(),
		).ValidateUpdate(t.Context(), old, identityCluster())
		require.ErrorContains(t, err, "mode is immutable")
	})
}

func TestLiteTemplateConsumerIdentity(t *testing.T) {
	for _, scenario := range []string{"rename", "cell change", "resource change", "implicit creation"} {
		t.Run(scenario, func(t *testing.T) {
			cluster := identityCluster()
			old := identityTemplate("original", "main")
			objects := []client.Object{cluster, old}
			incoming := old.DeepCopy()
			if scenario == "implicit creation" {
				cluster.Spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = ""
				incoming = identityTemplate("default", "other")
				objects = []client.Object{cluster}
			}
			if scenario == "rename" {
				incoming.Spec.Pools = identityTemplate("unused", "other").Spec.Pools
			}
			if scenario == "cell change" {
				p := incoming.Spec.Pools["main"]
				p.Cells = []multigresv1alpha1.CellName{"z2"}
				incoming.Spec.Pools["main"] = p
			}
			if scenario == "resource change" {
				p := incoming.Spec.Pools["main"]
				p.Storage.Size = "5Gi"
				incoming.Spec.Pools["main"] = p
			}
			validator := NewTemplateValidator(identityClient(objects...), "ShardTemplate")
			var err error
			if scenario == "implicit creation" {
				_, err = validator.ValidateCreate(t.Context(), incoming)
			} else {
				_, err = validator.ValidateUpdate(t.Context(), old, incoming)
			}
			if scenario == "resource change" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "identity is immutable")
			}
		})
	}
}

func TestLiteRejectsFallbackTemplateDeletion(t *testing.T) {
	cluster := identityCluster()
	cluster.Spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = ""
	tpl := identityTemplate("default", "main")
	validator := NewTemplateValidator(identityClient(cluster, tpl), "ShardTemplate")
	_, err := validator.ValidateDelete(t.Context(), tpl)
	require.ErrorContains(t, err, "identity is immutable")
}

// Inline shard fields overlay the effective template; they do not opt out of
// template inheritance. Admission must check both fallback and global defaults.
func TestLiteInlineTemplateConsumers(t *testing.T) {
	for _, templateName := range []string{"default", "global"} {
		operations := []string{"update"}
		// Named global templates must exist before the cluster can reference them.
		if templateName == "default" {
			operations = append(operations, "create")
		}
		for _, operation := range operations {
			for _, poolName := range []string{"main", "other"} {
				t.Run(templateName+"/"+operation+"/"+poolName, func(t *testing.T) {
					cluster := identityCluster()
					shard := &cluster.Spec.Databases[0].TableGroups[0].Shards[0]
					shard.ShardTemplate = ""
					shard.Spec = &multigresv1alpha1.ShardInlineSpec{
						Pools: identityTemplate("unused", "main").Spec.Pools,
					}
					if templateName == "global" {
						cluster.Spec.TemplateDefaults.ShardTemplate = "global"
					}
					objects := []client.Object{cluster}
					old := identityTemplate(templateName, "main")
					if operation == "update" {
						objects = append(objects, old)
					}
					incoming := identityTemplate(templateName, poolName)
					pool := incoming.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
					pool.Storage.Size = "5Gi"
					incoming.Spec.Pools[multigresv1alpha1.PoolName(poolName)] = pool
					validator := NewTemplateValidator(identityClient(objects...), "ShardTemplate")
					var err error
					if operation == "create" {
						_, err = validator.ValidateCreate(t.Context(), incoming)
					} else {
						_, err = validator.ValidateUpdate(t.Context(), old, incoming)
					}
					if poolName == "main" {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "exactly one resolved pool")
					}
				})
			}
		}
	}
}

func TestLiteInlineRejectsInheritedPoolDeletion(t *testing.T) {
	cluster := identityCluster()
	shard := &cluster.Spec.Databases[0].TableGroups[0].Shards[0]
	shard.ShardTemplate = ""
	shard.Spec = &multigresv1alpha1.ShardInlineSpec{}
	tpl := identityTemplate("default", "main")
	validator := NewTemplateValidator(identityClient(cluster, tpl), "ShardTemplate")
	_, err := validator.ValidateDelete(t.Context(), tpl)
	require.ErrorContains(t, err, "identity is immutable")
}
