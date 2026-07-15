//go:build e2e

package framework

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

var decoder runtime.Decoder

func init() {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	codecFactory := serializer.NewCodecFactory(scheme)
	decoder = codecFactory.UniversalDeserializer()
}

// LoadYAML reads a multi-document YAML file (path relative to repo root) and
// returns all decoded objects. Files with a single document return a slice of
// length 1.
func LoadYAML(repoRelPath string) ([]runtime.Object, error) {
	root, err := repoRoot()
	if err != nil {
		return nil, fmt.Errorf("repoRoot: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(root, repoRelPath))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", repoRelPath, err)
	}
	d := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var objs []runtime.Object
	for {
		var raw runtime.RawExtension
		if err := d.Decode(&raw); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("decode %s: %w", repoRelPath, err)
		}
		if raw.Raw == nil {
			continue
		}
		obj, _, err := decoder.Decode(raw.Raw, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", repoRelPath, err)
		}
		objs = append(objs, obj)
	}
	if len(objs) == 0 {
		return nil, fmt.Errorf("no objects found in %s", repoRelPath)
	}
	return objs, nil
}

// MustLoadCluster loads a MultigresCluster from a YAML file (path relative to
// repo root), overrides its namespace, and applies CI resource adjustments.
func MustLoadCluster(repoRelPath, namespace string) *multigresv1alpha1.MultigresCluster {
	objs, err := LoadYAML(repoRelPath)
	if err != nil {
		panic(err)
	}
	for _, obj := range objs {
		if cr, ok := obj.(*multigresv1alpha1.MultigresCluster); ok {
			cr.Namespace = namespace
			WithCIResources(&cr.Spec)
			return cr
		}
	}
	panic(fmt.Sprintf("%s: no *MultigresCluster found", repoRelPath))
}

// MustLoadCoreTemplate loads a CoreTemplate from a YAML file (path relative to
// repo root), overrides its namespace, and applies CI resources.
func MustLoadCoreTemplate(repoRelPath, namespace string) *multigresv1alpha1.CoreTemplate {
	objs, err := LoadYAML(repoRelPath)
	if err != nil {
		panic(err)
	}
	for _, obj := range objs {
		tmpl, ok := obj.(*multigresv1alpha1.CoreTemplate)
		if !ok {
			continue
		}
		tmpl.Namespace = namespace
		if tmpl.Spec.GlobalTopoServer != nil && tmpl.Spec.GlobalTopoServer.Etcd != nil {
			tmpl.Spec.GlobalTopoServer.Etcd.Resources = CIResources()
		}
		if tmpl.Spec.Multiadmin != nil {
			tmpl.Spec.Multiadmin.Resources = CIResources()
		}
		return tmpl
	}
	panic(fmt.Sprintf("%s: no *CoreTemplate found", repoRelPath))
}

// MustLoadCellTemplate loads a CellTemplate from a YAML file (path relative to
// repo root), overrides its namespace, and applies CI resources.
func MustLoadCellTemplate(repoRelPath, namespace string) *multigresv1alpha1.CellTemplate {
	objs, err := LoadYAML(repoRelPath)
	if err != nil {
		panic(err)
	}
	for _, obj := range objs {
		tmpl, ok := obj.(*multigresv1alpha1.CellTemplate)
		if !ok {
			continue
		}
		tmpl.Namespace = namespace
		if tmpl.Spec.Multigateway != nil {
			tmpl.Spec.Multigateway.Resources = CIResources()
		}
		return tmpl
	}
	panic(fmt.Sprintf("%s: no *CellTemplate found", repoRelPath))
}

// MustLoadShardTemplate loads a ShardTemplate from a YAML file (path relative
// to repo root), overrides its namespace, and applies CI resources.
func MustLoadShardTemplate(repoRelPath, namespace string) *multigresv1alpha1.ShardTemplate {
	objs, err := LoadYAML(repoRelPath)
	if err != nil {
		panic(err)
	}
	for _, obj := range objs {
		tmpl, ok := obj.(*multigresv1alpha1.ShardTemplate)
		if !ok {
			continue
		}
		tmpl.Namespace = namespace
		if tmpl.Spec.Multiorch != nil {
			tmpl.Spec.Multiorch.Resources = CIResources()
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
	panic(fmt.Sprintf("%s: no *ShardTemplate found", repoRelPath))
}
