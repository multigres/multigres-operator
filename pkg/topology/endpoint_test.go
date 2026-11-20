package topology

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestEndpointMatchesTopoServer(t *testing.T) {
	t.Parallel()

	ts := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-topo", Namespace: "infra"},
	}
	tests := []struct {
		name      string
		namespace string
		endpoint  string
		want      bool
	}{
		{"short name in namespace", "infra", "shared-topo", true},
		{"short name from another namespace", "tenant", "shared-topo:2379", false},
		{"namespaced", "tenant", "shared-topo.infra:2379", true},
		{"service", "tenant", "https://shared-topo.infra.svc:2379", true},
		{"fqdn without port", "tenant", "http://shared-topo.infra.svc.cluster.local", true},
		{"trailing dot", "tenant", "shared-topo.infra.svc.cluster.local.:2379", true},
		{"trailing slash", "tenant", "https://shared-topo.infra.svc.cluster.local.:2379/", true},
		{"headless short name in namespace", "infra", "shared-topo-headless:2379", true},
		{
			"headless short name from another namespace",
			"tenant",
			"shared-topo-headless:2379",
			false,
		},
		{"headless namespaced", "tenant", "shared-topo-headless.infra:2379", true},
		{"headless service", "tenant", "shared-topo-headless.infra.svc:2379", true},
		{
			"headless fqdn",
			"tenant",
			"https://shared-topo-headless.infra.svc.cluster.local:2379",
			true,
		},
		{"pod service", "tenant", "shared-topo-0.shared-topo-headless.infra.svc:2379", true},
		{
			"pod fqdn",
			"tenant",
			"https://shared-topo-12.shared-topo-headless.infra.svc.cluster.local.:2379",
			true,
		},
		{"wrong port", "tenant", "shared-topo.infra.svc:2380", false},
		{"path", "tenant", "https://shared-topo.infra.svc:2379/v3", false},
		{"userinfo", "tenant", "https://user@shared-topo.infra:2379", false},
		{"another server", "tenant", "other.infra.svc:2379", false},
		{
			"fqdn suffix collision",
			"tenant",
			"https://shared-topo.infra.svc.cluster.local.evil:2379",
			false,
		},
		{"nonnumeric pod", "tenant", "shared-topo-x.shared-topo-headless.infra.svc:2379", false},
		{
			"pod suffix collision",
			"tenant",
			"shared-topo-0.shared-topo-headless.infra.svc.evil:2379",
			false,
		},
		{
			"name prefix collision",
			"tenant",
			"shared-topo-extra-0.shared-topo-headless.infra.svc:2379",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := EndpointMatchesTopoServer(ts, tt.namespace, tt.endpoint); got != tt.want {
				t.Errorf("EndpointMatchesTopoServer() = %v, want %v", got, tt.want)
			}
		})
	}
}
