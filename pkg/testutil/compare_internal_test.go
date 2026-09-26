package testutil

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"

	"github.com/multigres/testkit/assert"
)

// TestFilterByFieldName tests the filter function directly.
func TestFilterByFieldName(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		fieldName string
		path      cmp.Path
		want      bool
	}{
		"Status - empty path": {
			fieldName: "Status",
			path:      cmp.Path{},
			want:      false,
		},
		"ObjectMeta - empty path": {
			fieldName: "ObjectMeta",
			path:      cmp.Path{},
			want:      false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			filter := filterByFieldName(tc.fieldName)
			got := filter(tc.path)
			assert.NewCollecting(t).
				Eq(tc.want, got, "filterByFieldName(%s) with empty path = %v, want", tc.fieldName, got)
		})
	}
}

// TestFilterByFieldName_Integration verifies it works with real Service objects.
func TestFilterByFieldName_Integration(t *testing.T) {
	t.Parallel()
	svc1 := &corev1.Service{
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{{IP: "1.2.3.4"}},
			},
		},
	}
	svc2 := &corev1.Service{
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{{IP: "5.6.7.8"}},
			},
		},
	}

	// Should match when ignoring Status
	diff := cmp.Diff(svc1, svc2, IgnoreStatus())
	assert.NewCollecting(t).
		Eq("", diff, "Services should match when ignoring Status, but found diff:\n")
}
