package testutil

import (
	"fmt"
	"io/fs"
	"os"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/client-go/applyconfigurations"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"sigs.k8s.io/yaml"
)

// TypeConverters returns the fake client's type converters with the CRD
// schemas in crdDir ahead of client-go's. Without a schema the fake deduces
// one, and a deduced CRD list is atomic, so a status condition applied by one
// manager is recorded as a claim on the whole conditions list instead of on
// its element, unlike on a real apiserver. Use it with WithTypeConverters
// whenever a test reads managedFields of a CRD.
//
// TODO(testkit): the fake's deduced CRD schema is a testkit gap; this helper
// can move there, or go once ctrltest's fake loads CRD schemas itself.
func TypeConverters(t testing.TB, crdDir string) []managedfields.TypeConverter {
	t.Helper()
	crds, err := ReadCRDs(crdDir)
	if err != nil {
		t.Fatal(err)
	}
	schemas := map[string]*spec.Schema{}
	for _, crd := range crds {
		for _, v := range crd.Spec.Versions {
			doc, err := builder.BuildOpenAPIV3(crd, v.Name, builder.Options{})
			if err != nil {
				t.Fatalf("build schema of %s %s: %v", crd.Name, v.Name, err)
			}
			for name, s := range doc.Components.Schemas {
				schemas[name] = s
			}
		}
	}
	converter, err := managedfields.NewTypeConverter(schemas, false)
	if err != nil {
		t.Fatalf("type converter from CRDs in %s: %v", crdDir, err)
	}
	return []managedfields.TypeConverter{
		converter,
		applyconfigurations.NewTypeConverter(clientgoscheme.Scheme),
	}
}

// ReadCRDs decodes every CRD manifest in dir.
func ReadCRDs(dir string) ([]*apiextensionsv1.CustomResourceDefinition, error) {
	fsys := os.DirFS(dir)
	files, err := fs.Glob(fsys, "*.yaml")
	if err != nil {
		return nil, fmt.Errorf("list CRDs in %s: %w", dir, err)
	}
	out := make([]*apiextensionsv1.CustomResourceDefinition, 0, len(files))
	for _, f := range files {
		raw, err := fs.ReadFile(fsys, f)
		if err != nil {
			return nil, err
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal(raw, crd); err != nil {
			return nil, fmt.Errorf("decode %s: %w", f, err)
		}
		out = append(out, crd)
	}
	return out, nil
}
