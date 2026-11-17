package backup_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/backup"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

func TestRetainedDefaultUsesResolvedLayout(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	cluster := &api.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "test"},
		Spec: api.MultigresClusterSpec{
			Cells:            []api.CellConfig{{Name: "east"}},
			TemplateDefaults: api.TemplateDefaults{ShardTemplate: "custom"},
			GlobalTopoServer: &api.GlobalTopoServerSpec{External: &api.ExternalTopoServerSpec{}},
		},
	}
	template := &api.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "custom", Namespace: cluster.Namespace},
		Spec: api.ShardTemplateSpec{
			Pools: map[api.PoolName]api.PoolSpec{"primary": {ReplicasPerCell: ptr.To(int32(1))}},
		},
	}
	identity := &api.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
		},
		Spec: api.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
		},
	}
	mounted := pvcutil.PoolDataName(identity, "primary", "east", 0)
	claim := func(name, prefix string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: cluster.Namespace,
			Annotations: map[string]string{api.AnnotationDefaultBackupKeyPrefix: prefix},
		}}
	}
	objects := []client.Object{template, claim(mounted, "clusters/original")}
	for _, unused := range []string{
		"data-restore-global-topo-0", // Now using external topology.
		pvcutil.PoolDataName(identity, "primary", "east", 1),
		pvcutil.PoolDataName(identity, "removed", "east", 0),
		pvcutil.PoolDataName(identity, "primary", "west", 0),
	} {
		pvc := claim(unused, "clusters/unused")
		pvc.Labels = identity.Labels
		objects = append(objects, pvc)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	name, prefix, err := backup.RetainedDefault(
		t.Context(),
		c,
		resolver.NewResolver(c, cluster.Namespace),
		cluster,
	)
	require.NoError(t, err)
	require.Equal(t, mounted, name)
	require.Equal(t, "clusters/original", prefix)
}
