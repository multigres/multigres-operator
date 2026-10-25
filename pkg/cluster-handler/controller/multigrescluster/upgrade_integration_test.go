//go:build integration
// +build integration

package multigrescluster_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// TestMultigresCluster_UpgradedGlobalServicePrunesExternalIPs drives the
// cluster controller over a global gateway Service last written under the
// child writer's name, the state an upgrade leaves it in, and disables the
// external gateway. The Service must lose its externalIPs: under any other
// name the old entry would stay a co-owner of them and nothing would prune
// them.
func TestMultigresCluster_UpgradedGlobalServicePrunesExternalIPs(t *testing.T) {
	t.Parallel()

	k8s, _, _ := setupIntegrationCore(t, nil, nil)
	ctx := t.Context()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "upgrade-gw", Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			ExternalGateway: &multigresv1alpha1.ExternalGatewayConfig{
				Enabled:     true,
				ExternalIPs: []multigresv1alpha1.IPAddress{"203.0.113.10"},
			},
		},
	}
	setTestPostgresPasswordSecretRef(cluster)
	require.NoError(t, k8s.Create(ctx, cluster))

	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: testNamespace, Name: cluster.Name + "-multigateway"}
	require.Eventually(t, func() bool {
		return k8s.Get(ctx, key, svc) == nil && len(svc.Spec.ExternalIPs) == 1
	}, 30*time.Second, 100*time.Millisecond, "global gateway Service never carried the external IP")
	testutil.RewriteAsChildWriter(t, k8s, svc)

	require.Eventually(t, func() bool {
		cur := &multigresv1alpha1.MultigresCluster{}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(cluster), cur); err != nil {
			return false
		}
		cur.Spec.ExternalGateway.Enabled = false
		return k8s.Update(ctx, cur) == nil
	}, 10*time.Second, 100*time.Millisecond, "disable the external gateway")

	require.Eventually(t, func() bool {
		return k8s.Get(ctx, key, svc) == nil && len(svc.Spec.ExternalIPs) == 0
	}, 30*time.Second, 100*time.Millisecond,
		"externalIPs survived disabling the external gateway: %v", svc.Spec.ExternalIPs)
	require.Equal(t, []string{ownership.ManagerChildWriter}, testutil.OperatorManagers(svc))
}
