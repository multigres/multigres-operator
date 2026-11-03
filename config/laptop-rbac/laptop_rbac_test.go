package laptoprbac

import (
	"os"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func readYAML(t *testing.T, path string, into any) {
	t.Helper()
	b, err := os.ReadFile(path) // #nosec G304 -- Only repository manifest paths are read.
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(b, into); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func grants(rules []rbacv1.PolicyRule, group, resource, verb string) bool {
	for _, r := range rules {
		if slices.Contains(r.APIGroups, group) &&
			slices.Contains(r.Resources, resource) &&
			slices.Contains(r.Verbs, verb) &&
			len(r.ResourceNames) == 0 {
			return true
		}
	}
	return false
}

// kubedialForward is the RBAC kubedial v0.1.0 documents for forward mode.
var kubedialForward = []struct{ group, resource, verb string }{
	{"", "pods/portforward", "create"},
	{"", "services", "get"},
	{"", "pods", "list"},
	{"discovery.k8s.io", "endpointslices", "list"},
}

func TestLaptopRoleCoversKubedialForwardMode(t *testing.T) {
	var role rbacv1.ClusterRole
	readYAML(t, "clusterrole.yaml", &role)
	for _, want := range kubedialForward {
		if !grants(role.Rules, want.group, want.resource, want.verb) {
			t.Errorf("laptop role lacks %s on %s (group %q)", want.verb, want.resource, want.group)
		}
	}
}

func TestLaptopRoleIsBoundToAGroupOnly(t *testing.T) {
	var binding rbacv1.ClusterRoleBinding
	readYAML(t, "clusterrolebinding.yaml", &binding)
	if len(binding.Subjects) != 1 {
		t.Fatalf("subjects = %v, want exactly one group", binding.Subjects)
	}
	s := binding.Subjects[0]
	if s.Kind != rbacv1.GroupKind || s.Name != "multigres-operator:laptop" {
		t.Fatalf("subject = %+v, want Group multigres-operator:laptop", s)
	}
}

// The deployed operator dials in-cluster addresses directly. Port-forwarding
// belongs to laptop mode alone, so it must not leak into the manager role
// through a +kubebuilder:rbac marker.
func TestManagerRoleHasNoPortForward(t *testing.T) {
	var role rbacv1.ClusterRole
	readYAML(t, "../rbac/role.yaml", &role)
	for _, r := range role.Rules {
		if slices.Contains(r.Resources, "pods/portforward") {
			t.Fatalf("manager role grants pods/portforward: %+v", r)
		}
	}
}
