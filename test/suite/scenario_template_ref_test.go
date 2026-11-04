package suite

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// TestClusterDropsItsLastTemplateReference removes the only ShardTemplate
// reference a cluster has. status.resolvedTemplates then names nothing, and
// the cluster must go on writing status: observedGeneration catches up with
// the edit and the namespace settles. A status apply that leaves
// resolvedTemplates as an empty object stores null, which the CRD rejects,
// and every status write after that fails.
func TestClusterDropsItsLastTemplateReference(t *testing.T) {
	c := newCase(t)
	tpl := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "only-template", Namespace: c.NS},
	}
	c.NoError(c.Create(tpl), "create ShardTemplate")
	cluster := c.newCluster("drop-template", func(spec *MultigresClusterSpec) {
		def := tableGroupConfig("default")
		def.Default = true
		def.Shards[0].ShardTemplate = multigresv1alpha1.TemplateRef(tpl.Name)
		spec.Databases = []DatabaseConfig{{
			Name: "postgres", Default: true, TableGroups: []TableGroupConfig{def},
		}}
	})
	c.WaitForClusterHealthy(cluster)

	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = ""
	})
	c.RequireQuiescent(10*time.Second, 90*time.Second)

	got := &MultigresCluster{}
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), got), "get cluster")
	c.True(got.Status.ObservedGeneration == got.Generation,
		"status stuck at generation %d of %d", got.Status.ObservedGeneration, got.Generation)
	c.True(got.Status.ResolvedTemplates == nil,
		"resolvedTemplates still names %+v", got.Status.ResolvedTemplates)
}
