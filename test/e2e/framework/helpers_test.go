//go:build e2e

package framework

import (
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

func TestMinimalFixtureUsesFailureSafeBootstrapCohort(t *testing.T) {
	c := assert.NewAborting(t)
	cluster := MustLoadCluster("config/samples/minimal.yaml", "test")
	pool := cluster.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"]
	c.NotNil(pool.ReplicasPerCell, "synthetic default pool replicasPerCell is nil")
	got, want := *pool.ReplicasPerCell, int32(3)
	c.Eq(want, got, "synthetic default pool replicasPerCell")
}

func TestTemplatedFixturePreservesReferences(t *testing.T) {
	c := assert.NewAborting(t)
	cluster := MustLoadCluster("test/e2e/fixtures/templated.yaml", "test")
	WithCIResources(&cluster.Spec) // Callers may apply resources more than once.
	c.EqDeep(multigresv1alpha1.TemplateRef("e2e-core"), cluster.Spec.TemplateDefaults.CoreTemplate)
	c.Nil(cluster.Spec.GlobalTopoServer)
	c.Nil(cluster.Spec.Multiadmin)
	c.Nil(cluster.Spec.MultiadminWeb)
	c.EqDeep(multigresv1alpha1.TemplateRef("e2e-cell"), cluster.Spec.Cells[0].CellTemplate)
	c.Nil(cluster.Spec.Cells[0].Spec)
	shard := cluster.Spec.Databases[0].TableGroups[0].Shards[0]
	c.EqDeep(multigresv1alpha1.TemplateRef("e2e-shard"), shard.ShardTemplate)
	c.Nil(shard.Spec)

	template := MustLoadShardTemplate("test/e2e/fixtures/templates/shard.yaml", "test")
	pool := template.Spec.Pools["default"]
	c.NotNil(pool.ReplicasPerCell)
	c.EqDeep(int32(3), *pool.ReplicasPerCell)
}

func TestWithCIResourcesPreservesTemplateConfiguration(t *testing.T) {
	for _, defaults := range []bool{false, true} {
		name := "explicit references"
		if defaults {
			name = "template defaults"
		}
		t.Run(name, func(t *testing.T) {
			c := assert.NewAborting(t)
			spec := multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{TemplateRef: "core"},
				Multiadmin:       &multigresv1alpha1.MultiadminConfig{TemplateRef: "core"},
				MultiadminWeb:    &multigresv1alpha1.MultiadminWebConfig{TemplateRef: "core"},
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "cell", ZoneID: "us-central1-a", CellTemplate: "cell"},
				},
				Databases: []multigresv1alpha1.DatabaseConfig{
					{Name: "postgres", TableGroups: []multigresv1alpha1.TableGroupConfig{
						{
							Name: "default",
							Shards: []multigresv1alpha1.ShardConfig{
								{Name: "0-inf", ShardTemplate: "shard"},
							},
						},
					}},
				},
			}
			if defaults {
				spec.TemplateDefaults = multigresv1alpha1.TemplateDefaults{
					CoreTemplate:  "core",
					CellTemplate:  "cell",
					ShardTemplate: "shard",
				}
				spec.GlobalTopoServer, spec.Multiadmin, spec.MultiadminWeb = nil, nil, nil
				spec.Cells[0].CellTemplate = ""
				spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = ""
			}
			before := spec.DeepCopy()
			WithCIResources(&spec)
			WithCIResources(&spec)
			c.EqDeep(before, &spec)
			if defaults {
				spec.Databases[0].TableGroups[0].Shards = nil
				WithCIResources(&spec)
				c.Nil(
					spec.Databases[0].TableGroups[0].Shards[0].Spec,
					"a synthesized shard must still inherit its default template",
				)
			}
		})
	}
}

func TestWithCIResourcesPreservesOverridesAndExternalTopo(t *testing.T) {
	c := assert.NewAborting(t)
	spec := multigresv1alpha1.MultigresClusterSpec{
		GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
			External: &multigresv1alpha1.ExternalTopoServerSpec{
				Endpoints: []multigresv1alpha1.EndpointUrl{"http://etcd:2379"},
			},
		},
		Cells: []multigresv1alpha1.CellConfig{
			{Name: "cell", Overrides: &multigresv1alpha1.CellOverrides{}},
		},
		Databases: []multigresv1alpha1.DatabaseConfig{
			{Name: "postgres", TableGroups: []multigresv1alpha1.TableGroupConfig{
				{
					Name: "default",
					Shards: []multigresv1alpha1.ShardConfig{
						{Name: "0-inf", Overrides: &multigresv1alpha1.ShardOverrides{}},
					},
				},
			}},
		},
	}
	WithCIResources(&spec)
	c.Nil(spec.GlobalTopoServer.Etcd)
	c.EqDeep(
		[]multigresv1alpha1.EndpointUrl{"http://etcd:2379"},
		spec.GlobalTopoServer.External.Endpoints,
	)
	c.Nil(spec.Cells[0].Spec)
	c.NotNil(spec.Cells[0].Overrides)
	c.Nil(spec.Databases[0].TableGroups[0].Shards[0].Spec)
	c.NotNil(spec.Databases[0].TableGroups[0].Shards[0].Overrides)
}

func TestWithCIResourcesPreservesInlineConfiguration(t *testing.T) {
	c := assert.NewAborting(t)
	cluster := MustLoadCluster("config/samples/no-templates.yaml", "test")
	spec := &cluster.Spec
	before := spec.DeepCopy()
	// Explicit inline configuration still wins over template defaults.
	spec.TemplateDefaults = multigresv1alpha1.TemplateDefaults{
		CoreTemplate:  "core",
		CellTemplate:  "cell",
		ShardTemplate: "shard",
	}
	WithCIResources(spec)
	c.EqDeep(before.GlobalTopoServer, spec.GlobalTopoServer)
	c.EqDeep(before.Multiadmin, spec.Multiadmin)
	c.EqDeep(before.MultiadminWeb, spec.MultiadminWeb)
	c.EqDeep(before.Cells, spec.Cells)
	c.EqDeep(before.Databases, spec.Databases)
}
