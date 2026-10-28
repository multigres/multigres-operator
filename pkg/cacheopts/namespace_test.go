package cacheopts_test

import (
	"testing"

	"github.com/multigres/multigres-operator/pkg/cacheopts"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

func TestNamespaceIsolationCoversEveryOverride(t *testing.T) {
	opts := cacheopts.New("lite-system", "", "lite-a,lite-b")
	if len(opts.DefaultNamespaces) != 3 {
		t.Fatalf("unexpected namespaces: %v", opts.DefaultNamespaces)
	}
	for obj, config := range opts.ByObject {
		if _, all := config.Namespaces[cache.AllNamespaces]; all {
			t.Fatalf("%T still watches all namespaces", obj)
		}
		if len(config.Namespaces) != 3 {
			t.Fatalf("%T has inconsistent namespace scope", obj)
		}
		for _, namespace := range []string{"lite-system", "lite-a", "lite-b"} {
			if _, ok := config.Namespaces[namespace]; !ok {
				t.Fatalf("%T missing %s", obj, namespace)
			}
		}
	}
	global := cacheopts.New("operator", "")
	if global.DefaultNamespaces != nil {
		t.Fatal("empty scope must preserve existing cluster-wide behavior")
	}
	for obj, config := range global.ByObject {
		if _, ok := config.Namespaces[cache.AllNamespaces]; !ok {
			t.Fatalf("default %T lost its global watch", obj)
		}
	}
}
