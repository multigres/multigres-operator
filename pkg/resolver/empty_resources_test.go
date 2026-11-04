package resolver

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// emptyResourceShapes are the non-zero ways a resources block can ask for
// nothing. Each overrides inheritance and defaulting: it resolves to a block
// that is still set (not the zero value, so the next resolution of what the
// defaulting webhook writes back still sees it as set) and holds no entries.
// The child builders then omit it, through ForApply.
func emptyResourceShapes() map[string]corev1.ResourceRequirements {
	return map[string]corev1.ResourceRequirements{
		"empty requests": {Requests: corev1.ResourceList{}},
		"empty limits":   {Limits: corev1.ResourceList{}},
		"empty claims":   {Claims: []corev1.ResourceClaim{}},
		"all empty": {
			Requests: corev1.ResourceList{},
			Limits:   corev1.ResourceList{},
			Claims:   []corev1.ResourceClaim{},
		},
	}
}

// requireResolvedEmpty fails unless res asks for nothing and is still set.
func requireResolvedEmpty(t *testing.T, what string, res corev1.ResourceRequirements) {
	t.Helper()
	if len(res.Requests) != 0 || len(res.Limits) != 0 || len(res.Claims) != 0 {
		t.Errorf("%s resources = %#v, want no entries (defaults must not apply)", what, res)
	}
	if isResourcesZero(res) {
		t.Errorf("%s resources lost the explicitly empty block: %#v", what, res)
	}
}

func TestResolveShard_EmptyResources(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	r := NewResolver(fake.NewClientBuilder().WithScheme(scheme).Build(), "default")

	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		for name, shape := range emptyResourceShapes() {
			shardCfg := &multigresv1alpha1.ShardConfig{
				Name: "0-inf",
				Spec: &multigresv1alpha1.ShardInlineSpec{
					Multiorch: multigresv1alpha1.MultiorchSpec{
						StatelessSpec: multigresv1alpha1.StatelessSpec{
							Resources: *shape.DeepCopy(),
						},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"default": {
							Type: "readWrite",
							Postgres: multigresv1alpha1.ContainerConfig{
								Resources: *shape.DeepCopy(),
							},
							Multipooler: multigresv1alpha1.ContainerConfig{
								Resources: *shape.DeepCopy(),
							},
							PgBackRestResources: *shape.DeepCopy(),
						},
					},
				},
			}
			got, err := r.ResolveShard(t.Context(), shardCfg, ResolveShardOptions{
				AllCellNames:            []multigresv1alpha1.CellName{"zone-a"},
				MaterializeCellDefaults: true,
				Mode:                    mode,
			})
			if err != nil {
				t.Fatalf("mode %q, %s: ResolveShard() error = %v", mode, name, err)
			}
			prefix := "mode " + mode + ", " + name + ": "
			requireResolvedEmpty(t, prefix+"multiorch", got.Multiorch.Resources)
			requireResolvedEmpty(t, prefix+"postgres", got.Pools["default"].Postgres.Resources)
			requireResolvedEmpty(
				t,
				prefix+"multipooler",
				got.Pools["default"].Multipooler.Resources,
			)
			requireResolvedEmpty(
				t,
				prefix+"pgbackrest",
				got.Pools["default"].PgBackRestResources,
			)
		}
	}
}

func TestResolveCell_EmptyResources(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	r := NewResolver(fake.NewClientBuilder().WithScheme(scheme).Build(), "default")

	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
			Spec:       multigresv1alpha1.MultigresClusterSpec{Mode: mode},
		}
		for name, shape := range emptyResourceShapes() {
			gateway, _, localTopo, err := r.ResolveCell(t.Context(), cluster,
				&multigresv1alpha1.CellConfig{
					Name: "zone-a",
					Spec: &multigresv1alpha1.CellInlineSpec{
						Multigateway: multigresv1alpha1.MultigatewaySpec{
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Resources: *shape.DeepCopy(),
							},
						},
						LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
							Etcd: &multigresv1alpha1.EtcdSpec{Resources: *shape.DeepCopy()},
						},
					},
				})
			if err != nil {
				t.Fatalf("mode %q, %s: ResolveCell() error = %v", mode, name, err)
			}
			prefix := "mode " + mode + ", " + name + ": "
			requireResolvedEmpty(t, prefix+"multigateway", gateway.Resources)
			requireResolvedEmpty(t, prefix+"local etcd", localTopo.Etcd.Resources)
		}
	}
}

func TestResolveGlobalTopo_EmptyResources(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	r := NewResolver(fake.NewClientBuilder().WithScheme(scheme).Build(), "default")

	for name, shape := range emptyResourceShapes() {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Resources: *shape.DeepCopy()},
				},
			},
		}
		got, err := r.ResolveGlobalTopo(t.Context(), cluster)
		if err != nil {
			t.Fatalf("%s: ResolveGlobalTopo() error = %v", name, err)
		}
		requireResolvedEmpty(t, name+": global etcd", got.Etcd.Resources)
	}
}

// TestResolveShard_BareResourcesBlockInherits is the contrast to the empty
// shapes above: `resources: {}` decodes to the zero value, which is the same
// as leaving the block out, so it inherits and is defaulted.
func TestResolveShard_BareResourcesBlockInherits(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	r := NewResolver(fake.NewClientBuilder().WithScheme(scheme).Build(), "default")
	got, err := r.ResolveShard(t.Context(), &multigresv1alpha1.ShardConfig{
		Name: "0-inf",
		Spec: &multigresv1alpha1.ShardInlineSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {Type: "readWrite"},
			},
		},
	}, ResolveShardOptions{
		AllCellNames:            []multigresv1alpha1.CellName{"zone-a"},
		MaterializeCellDefaults: true,
	})
	if err != nil {
		t.Fatalf("ResolveShard() error = %v", err)
	}
	pool := got.Pools["default"]
	for what, res := range map[string]corev1.ResourceRequirements{
		"multiorch": got.Multiorch.Resources,
		"postgres":  pool.Postgres.Resources,
	} {
		if len(res.Requests) == 0 && len(res.Limits) == 0 {
			t.Errorf("%s resources = %#v, want the defaults", what, res)
		}
	}
}
