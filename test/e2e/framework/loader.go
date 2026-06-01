//go:build e2e

package framework

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

var decoder runtime.Decoder

func init() {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	codecFactory := serializer.NewCodecFactory(scheme)
	decoder = codecFactory.UniversalDeserializer()
}

// LoadYAML reads a YAML file at the given path (relative to the repo root)
// and decodes it into a runtime.Object. The returned object is the concrete
// multigres API type (e.g., *MultigresCluster, *CoreTemplate).
func LoadYAML(repoRelPath string) (runtime.Object, error) {
	root, err := repoRoot()
	if err != nil {
		return nil, fmt.Errorf("repoRoot: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(root, repoRelPath))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", repoRelPath, err)
	}
	obj, _, err := decoder.Decode(data, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", repoRelPath, err)
	}
	return obj, nil
}

// MustLoadCluster loads a MultigresCluster from a YAML file (path relative to
// repo root), overrides its namespace, and applies CI resource adjustments.
func MustLoadCluster(repoRelPath, namespace string) *multigresv1alpha1.MultigresCluster {
	obj, err := LoadYAML(repoRelPath)
	if err != nil {
		panic(err)
	}
	cr, ok := obj.(*multigresv1alpha1.MultigresCluster)
	if !ok {
		panic(fmt.Sprintf("%s is %T, want *MultigresCluster", repoRelPath, obj))
	}
	cr.Namespace = namespace
	WithCIResources(&cr.Spec)
	return cr
}

// MustLoadCoreTemplate loads a CoreTemplate from a YAML file (path relative to
// repo root), overrides its namespace, and applies CI resources.
func MustLoadCoreTemplate(repoRelPath, namespace string) *multigresv1alpha1.CoreTemplate {
	obj, err := LoadYAML(repoRelPath)
	if err != nil {
		panic(err)
	}
	tmpl, ok := obj.(*multigresv1alpha1.CoreTemplate)
	if !ok {
		panic(fmt.Sprintf("%s is %T, want *CoreTemplate", repoRelPath, obj))
	}
	tmpl.Namespace = namespace
	// Apply CI resources to template specs.
	if tmpl.Spec.GlobalTopoServer != nil && tmpl.Spec.GlobalTopoServer.Etcd != nil {
		tmpl.Spec.GlobalTopoServer.Etcd.Resources = CIResources()
	}
	if tmpl.Spec.MultiAdmin != nil {
		tmpl.Spec.MultiAdmin.Resources = CIResources()
	}
	return tmpl
}

// MustLoadCellTemplate loads a CellTemplate from a YAML file (path relative to
// repo root), overrides its namespace, and applies CI resources.
func MustLoadCellTemplate(repoRelPath, namespace string) *multigresv1alpha1.CellTemplate {
	obj, err := LoadYAML(repoRelPath)
	if err != nil {
		panic(err)
	}
	tmpl, ok := obj.(*multigresv1alpha1.CellTemplate)
	if !ok {
		panic(fmt.Sprintf("%s is %T, want *CellTemplate", repoRelPath, obj))
	}
	tmpl.Namespace = namespace
	if tmpl.Spec.MultiGateway != nil {
		tmpl.Spec.MultiGateway.Resources = CIResources()
	}
	return tmpl
}

// MustLoadShardTemplate loads a ShardTemplate from a YAML file (path relative
// to repo root), overrides its namespace, and applies CI resources.
func MustLoadShardTemplate(repoRelPath, namespace string) *multigresv1alpha1.ShardTemplate {
	obj, err := LoadYAML(repoRelPath)
	if err != nil {
		panic(err)
	}
	tmpl, ok := obj.(*multigresv1alpha1.ShardTemplate)
	if !ok {
		panic(fmt.Sprintf("%s is %T, want *ShardTemplate", repoRelPath, obj))
	}
	tmpl.Namespace = namespace
	// CI resources on multiorch.
	if tmpl.Spec.MultiOrch != nil {
		tmpl.Spec.MultiOrch.Resources = CIResources()
	}
	// CI resources on each pool. No fsGroup override: the operator defaults
	// pool containers to a numeric uid, so e2e exercises that default.
	for name, pool := range tmpl.Spec.Pools {
		pool.Postgres = CIContainerConfig()
		pool.Multipooler = CIContainerConfig()
		tmpl.Spec.Pools[name] = pool
	}
	return tmpl
}
