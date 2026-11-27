package topology

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

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

func TestEndpointTopoServerKeys(t *testing.T) {
	t.Parallel()

	key := func(namespace, name string) client.ObjectKey {
		return client.ObjectKey{Namespace: namespace, Name: name}
	}
	tests := []struct {
		name      string
		namespace string
		endpoint  string
		want      []client.ObjectKey
	}{
		{"short name", "tenant", "shared-topo", []client.ObjectKey{key("tenant", "shared-topo")}},
		{
			"namespaced",
			"tenant",
			"shared-topo.infra:2379",
			[]client.ObjectKey{key("infra", "shared-topo")},
		},
		{
			"service",
			"tenant",
			"https://shared-topo.infra.svc:2379",
			[]client.ObjectKey{key("infra", "shared-topo")},
		},
		{
			"headless",
			"tenant",
			"https://shared-topo-headless.infra.svc.cluster.local",
			[]client.ObjectKey{key("infra", "shared-topo-headless"), key("infra", "shared-topo")},
		},
		{
			"pod",
			"tenant",
			"https://shared-topo-0.shared-topo-headless.infra.svc:2379",
			[]client.ObjectKey{key("infra", "shared-topo-headless"), key("infra", "shared-topo")},
		},
		{
			"pod in a namespace named svc",
			"tenant",
			"https://topo-0.topo-headless.svc.svc:2379",
			[]client.ObjectKey{key("svc", "topo-headless"), key("svc", "topo")},
		},
		{
			"pod in a namespace named svc, cluster.local",
			"tenant",
			"https://topo-0.topo-headless.svc.svc.cluster.local:2379",
			[]client.ObjectKey{key("svc", "topo-headless"), key("svc", "topo")},
		},
		{
			"service in a namespace named svc",
			"tenant",
			"https://topo.svc.svc:2379",
			[]client.ObjectKey{key("svc", "topo")},
		},
		{
			"service in a namespace named svc, cluster.local",
			"tenant",
			"https://topo.svc.svc.cluster.local:2379",
			[]client.ObjectKey{key("svc", "topo")},
		},
		{"wrong port", "tenant", "shared-topo.infra:2380", nil},
		{"user info", "tenant", "https://user@shared-topo.infra:2379", nil},
		{"path", "tenant", "https://shared-topo.infra:2379/v3", nil},
		{"external host", "tenant", "https://etcd.example.com.internal.corp:2379", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := EndpointTopoServerKeys(tt.namespace, tt.endpoint)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("EndpointTopoServerKeys(%q, %q) = %v, want %v",
					tt.namespace, tt.endpoint, got, tt.want)
			}
		})
	}
}

func TestEndpointTopoServerKeys_NamesEndingInHeadless(t *testing.T) {
	t.Parallel()

	shared := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "infra"},
	}
	sharedHeadless := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-headless", Namespace: "infra"},
	}
	tests := []struct {
		name     string
		endpoint string
		server   *multigresv1alpha1.TopoServer
	}{
		{
			"client Service of shared-headless",
			"https://shared-headless.infra.svc:2379",
			sharedHeadless,
		},
		{
			"headless Service of shared-headless",
			"https://shared-headless-headless.infra.svc:2379",
			sharedHeadless,
		},
		{
			"pod of shared-headless",
			"https://shared-headless-0.shared-headless-headless.infra.svc:2379",
			sharedHeadless,
		},
		{"headless Service of shared", "https://shared-headless.infra.svc:2379", shared},
		{"pod of shared", "https://shared-0.shared-headless.infra.svc:2379", shared},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var matched []string
			for _, key := range EndpointTopoServerKeys("tenant", tt.endpoint) {
				candidate := &multigresv1alpha1.TopoServer{
					ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
				}
				if EndpointMatchesTopoServer(candidate, "tenant", tt.endpoint) {
					matched = append(matched, key.Name)
				}
			}
			if !slices.Contains(matched, tt.server.Name) {
				t.Fatalf("candidates matching %q = %v, want %q among them",
					tt.endpoint, matched, tt.server.Name)
			}
		})
	}
}
