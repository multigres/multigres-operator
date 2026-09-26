//go:build e2e

package framework

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/multigres/multigres/go/common/consensus"
	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"

	"github.com/multigres/testkit/assert"
)

// Check every shared fixture and every sample consumed by the shared/dedicated
// suites without starting Kubernetes. This tests resolved shard-wide capacity,
// including template defaults and pool/cell placement, not replicas per pool.
func TestE2EFixturesSupportBootstrap(t *testing.T) {
	ck := assert.NewAborting(t)
	root, err := repoRoot()
	ck.NoError(err)
	scheme := runtime.NewScheme()
	ck.NoError(multigresv1alpha1.AddToScheme(scheme))
	ck.NoError(corev1.AddToScheme(scheme))
	strict := serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDeserializer()
	decode := func(t *testing.T, path string) []runtime.Object {
		t.Helper()
		c := assert.NewAborting(t)
		data, err := os.ReadFile(
			path,
		) // #nosec G304 -- Only repository fixture paths enumerated below are read.
		c.NoError(err)
		documents := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
		var objects []runtime.Object
		for {
			var raw runtime.RawExtension
			if err := documents.Decode(&raw); err == io.EOF {
				break
			} else {
				c.NoError(err)
			}
			if len(raw.Raw) == 0 {
				continue
			}
			object, _, err := strict.Decode(raw.Raw, nil, nil)
			c.NoError(err, "%s must use current API fields", path)
			objects = append(objects, object)
		}
		return objects
	}
	var templates []client.Object
	for _, directory := range []string{"test/e2e/fixtures/templates", "config/samples/templates"} {
		paths, err := filepath.Glob(filepath.Join(root, directory, "*.yaml"))
		ck.NoError(err)
		for _, path := range paths {
			objects := decode(t, path)
			ck.Len(objects, 1)
			template := objects[0].(client.Object)
			template.SetNamespace("test")
			templates = append(templates, template)
		}
	}
	paths, err := filepath.Glob(filepath.Join(root, "test/e2e/fixtures/*.yaml"))
	ck.NoError(err)
	for _, sample := range []string{"minimal.yaml", "no-templates.yaml", "templated-cluster.yaml"} {
		paths = append(paths, filepath.Join(root, "config/samples", sample))
	}
	for _, path := range paths {
		relative, err := filepath.Rel(root, path)
		ck.NoError(err)
		t.Run(relative, func(t *testing.T) {
			ck := assert.NewAborting(t)
			decode(t, path)
			cluster := MustLoadCluster(relative, "test")
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(templates...).Build()
			r := resolver.NewResolver(c, "test")
			_, err = r.PopulateClusterDefaults(context.Background(), cluster)
			ck.NoError(err)
			var cells []multigresv1alpha1.CellName
			for _, cell := range cluster.Spec.Cells {
				cells = append(cells, cell.Name)
			}
			for _, database := range cluster.Spec.Databases {
				policyName := database.DurabilityPolicy
				if policyName == "" {
					policyName = cluster.Spec.DurabilityPolicy
				}
				policyProto, err := consensus.ParseUserSpecifiedDurabilityPolicy(policyName)
				ck.NoError(err)
				policy, err := consensus.NewPolicyFromProto(policyProto)
				ck.NoError(err)
				for _, group := range database.TableGroups {
					for _, shard := range group.Shards {
						if shard.ShardTemplate == "" {
							shard.ShardTemplate = cluster.Spec.TemplateDefaults.ShardTemplate
						}
						resolved, err := r.ResolveShard(
							context.Background(),
							&shard,
							resolver.ResolveShardOptions{
								AllCellNames: cells, MaterializeCellDefaults: true,
							},
						)
						ck.NoError(err)
						var cohort []*clustermetadata.ID
						for name, pool := range resolved.Pools {
							for _, cell := range pool.Cells {
								for i := int32(0); i < *pool.ReplicasPerCell; i++ {
									cohort = append(
										cohort,
										&clustermetadata.ID{
											Cell: string(cell),
											Name: fmt.Sprintf("%s-%d", name, i),
										},
									)
								}
							}
						}
						ck.True(
							consensus.CohortSurvivesAnyMemberLoss(policy, cohort),
							"%s/%s/%s: %d poolers cannot safely bootstrap %s",
							database.Name,
							group.Name,
							shard.Name,
							len(cohort),
							policyName,
						)
					}
				}
			}
		})
	}
}
