package multigrescluster

import (
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestClusterMode(t *testing.T) {
	tests := map[string]struct {
		mode string
		want string
	}{
		"empty is distributed": {mode: "", want: multigresv1alpha1.ModeDistributed},
		"lite": {
			mode: multigresv1alpha1.ModeLite,
			want: multigresv1alpha1.ModeLite,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := &multigresv1alpha1.MultigresCluster{}
			c.Spec.Mode = tc.mode
			if got := clusterMode(c); got != tc.want {
				t.Errorf("clusterMode() = %q, want %q", got, tc.want)
			}
		})
	}
}
