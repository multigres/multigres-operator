package suite

import (
	"fmt"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestLiteClusterViewsResolution(t *testing.T) {
	gateway := func(enabled bool) *multigresv1alpha1.MultigatewaySpec {
		return &multigresv1alpha1.MultigatewaySpec{
			ClusterViews: &multigresv1alpha1.GatewayClusterViewsConfig{Enabled: ptr.To(enabled)},
		}
	}
	for _, tt := range []struct {
		name                                         string
		fallback, global, explicit, override, inline *bool
		want                                         bool
	}{
		{name: "unset"},
		{name: "fallback", fallback: ptr.To(true), want: true},
		{name: "global", fallback: ptr.To(false), global: ptr.To(true), want: true},
		{name: "explicit", global: ptr.To(false), explicit: ptr.To(true), want: true},
		{name: "override enabled", explicit: ptr.To(false), override: ptr.To(true), want: true},
		{name: "override disabled", explicit: ptr.To(true), override: ptr.To(false)},
		{name: "inline enabled", global: ptr.To(false), inline: ptr.To(true), want: true},
		{name: "inline disabled", global: ptr.To(true), inline: ptr.To(false)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newCase(t)
			for name, enabled := range map[string]*bool{"default": tt.fallback, "global": tt.global, "explicit": tt.explicit} {
				if enabled != nil {
					c.NoError(c.Create(&multigresv1alpha1.CellTemplate{
						ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.NS},
						Spec: multigresv1alpha1.CellTemplateSpec{
							Multigateway: gateway(*enabled),
						},
					}))
				}
			}
			c.newCluster("lite", func(spec *MultigresClusterSpec) {
				spec.Mode = multigresv1alpha1.ModeLite
				if tt.global != nil {
					spec.TemplateDefaults.CellTemplate = "global"
				}
				if tt.explicit != nil {
					spec.Cells[0].CellTemplate = "explicit"
				}
				if tt.override != nil {
					spec.Cells[0].Overrides = &multigresv1alpha1.CellOverrides{
						Multigateway: gateway(*tt.override),
					}
				}
				if tt.inline != nil {
					spec.Cells[0].Spec = &multigresv1alpha1.CellInlineSpec{
						Multigateway: *gateway(*tt.inline),
					}
				}
			})
			c.Eventually(30*time.Second, "resolved cluster views on Lite pod", func() error {
				pods := &corev1.PodList{}
				if err := c.List(
					pods,
					client.MatchingLabels{
						metadata.LabelMultigresGatewayRole: metadata.GatewayRoleLite,
					},
				); err != nil {
					return err
				}
				if len(pods.Items) != 1 {
					return fmt.Errorf("pods=%d", len(pods.Items))
				}
				for _, container := range pods.Items[0].Spec.Containers {
					if container.Name == "minigres" {
						got := slices.Contains(container.Args, "--enable-cluster-views=true")
						if got != tt.want {
							return fmt.Errorf("cluster views enabled=%v, want %v", got, tt.want)
						}
						return nil
					}
				}
				return fmt.Errorf("Minigres container missing")
			})
		})
	}
}
