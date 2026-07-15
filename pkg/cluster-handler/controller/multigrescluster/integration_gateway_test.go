//go:build integration
// +build integration

package multigrescluster_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const pollInterval = 200 * time.Millisecond

func TestExternalGateway_EnableDisableLifecycle(t *testing.T) {
	t.Parallel()

	const clusterName = "gw-lifecycle"

	k8sClient, watcher := setupIntegration(t)

	// Step 1: Create cluster with externalGateway enabled and annotations.
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: testNamespace,
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate:  "default",
				CellTemplate:  "default",
				ShardTemplate: "default",
			},
			Cells: []multigresv1alpha1.CellConfig{
				{Name: "zone-a", ZoneID: "use1-az1"},
			},
			ExternalGateway: &multigresv1alpha1.ExternalGatewayConfig{
				Enabled:     true,
				ExternalIPs: []multigresv1alpha1.IPAddress{"2001:db8::100"},
				Annotations: map[string]string{
					"team.example.com/owner": "platform-engineering",
				},
			},
		},
	}

	setTestPostgresPasswordSecretRef(cluster)

	require.NoError(t, k8sClient.Create(t.Context(), cluster))

	// Add extra comparison options for Service runtime fields.
	watcher.SetCmpOpts(
		testutil.IgnoreMetaRuntimeFields(),
		testutil.IgnoreServiceRuntimeFields(),
		cmpopts.IgnoreFields(corev1.ServiceSpec{}, "ExternalTrafficPolicy"),
		cmpopts.IgnoreFields(corev1.ServicePort{}, "NodePort"),
	)

	// Step 2: Verify the global Service is created as ClusterIP with externalIPs + annotations.
	gwLabels := clusterLabels(t, clusterName, "multigateway", "")
	expectedGwSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName + "-multigateway",
			Namespace: testNamespace,
			Labels:    gwLabels,
			Annotations: map[string]string{
				"team.example.com/owner": "platform-engineering",
			},
			OwnerReferences: clusterOwnerRefs(t, clusterName),
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				metadata.LabelAppComponent: metadata.ComponentMultigateway,
				metadata.LabelAppInstance:  clusterName,
			},
			Type:        corev1.ServiceTypeClusterIP,
			ExternalIPs: []string{"2001:db8::100"},
			Ports: []corev1.ServicePort{
				{
					Name:       "postgres",
					Port:       5432,
					TargetPort: intstr.FromString("postgres"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	expectedReplicaGwSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            clusterName + "-multigateway-replica",
			Namespace:       testNamespace,
			Labels:          gwLabels,
			OwnerReferences: clusterOwnerRefs(t, clusterName),
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				metadata.LabelAppComponent: metadata.ComponentMultigateway,
				metadata.LabelAppInstance:  clusterName,
			},
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "pg-replica",
					Port:       5433,
					TargetPort: intstr.FromString("pg-replica"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	require.NoError(t, watcher.WaitForMatch(expectedGwSvc),
		"global multigateway Service should be ClusterIP with externalIPs and annotations")
	require.NoError(t, watcher.WaitForMatch(expectedReplicaGwSvc),
		"global multigateway replica Service should be ClusterIP")

	// Step 3: Verify initial condition is NoReadyGateways (endpoint assigned via externalIP, 0 ready gateways).
	assert.Eventually(t, func() bool {
		var mgc multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgc); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(mgc.Status.Conditions, multigresv1alpha1.ConditionGatewayExternalReady)
		return cond != nil &&
			cond.Status == metav1.ConditionFalse &&
			cond.Reason == multigresv1alpha1.ReasonNoReadyGateways &&
			mgc.Status.Gateway != nil &&
			mgc.Status.Gateway.ExternalEndpoint == "2001:db8::100"
	}, testTimeout, pollInterval, "condition should be False/NoReadyGateways before gateways are ready")

	// Step 4: Simulate Cell reporting ready gateways by updating Cell status.
	var cellList multigresv1alpha1.CellList
	require.NoError(t, k8sClient.List(t.Context(), &cellList,
		client.InNamespace(testNamespace),
		client.MatchingLabels{"multigres.com/cluster": clusterName},
	))
	require.NotEmpty(t, cellList.Items, "expected at least one Cell CR")

	cell := &cellList.Items[0]
	cell.Status.GatewayReadyReplicas = 1
	cell.Status.GatewayReplicas = 1
	cell.Status.ObservedGeneration = cell.Generation
	require.NoError(t, k8sClient.Status().Update(t.Context(), cell),
		"simulating Cell reporting ready gateway replicas")

	// Step 5: Verify condition transitions to EndpointReady.
	assert.Eventually(t, func() bool {
		var mgc multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgc); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(mgc.Status.Conditions, multigresv1alpha1.ConditionGatewayExternalReady)
		return cond != nil &&
			cond.Status == metav1.ConditionTrue &&
			cond.Reason == multigresv1alpha1.ReasonEndpointReady &&
			mgc.Status.Gateway != nil &&
			mgc.Status.Gateway.ExternalEndpoint == "2001:db8::100"
	}, testTimeout, pollInterval, "condition should be True/EndpointReady with endpoint in status")

	// Verify observedGeneration is set on the condition.
	var mgcCheck multigresv1alpha1.MultigresCluster
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgcCheck))
	cond := meta.FindStatusCondition(mgcCheck.Status.Conditions, multigresv1alpha1.ConditionGatewayExternalReady)
	require.NotNil(t, cond)
	assert.Equal(t, mgcCheck.Generation, cond.ObservedGeneration,
		"observedGeneration should match cluster generation")

	// Step 6: Disable external gateway and verify reversion.
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
	cluster.Spec.ExternalGateway = &multigresv1alpha1.ExternalGatewayConfig{
		Enabled: false,
	}
	require.NoError(t, k8sClient.Update(t.Context(), cluster),
		"disabling external gateway")

	// Step 7: Verify Service reverts to ClusterIP with no gateway annotations.
	expectedClusterIPSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            clusterName + "-multigateway",
			Namespace:       testNamespace,
			Labels:          gwLabels,
			OwnerReferences: clusterOwnerRefs(t, clusterName),
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				metadata.LabelAppComponent: metadata.ComponentMultigateway,
				metadata.LabelAppInstance:  clusterName,
			},
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "postgres",
					Port:       5432,
					TargetPort: intstr.FromString("postgres"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	expectedReplicaClusterIPSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            clusterName + "-multigateway-replica",
			Namespace:       testNamespace,
			Labels:          gwLabels,
			OwnerReferences: clusterOwnerRefs(t, clusterName),
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				metadata.LabelAppComponent: metadata.ComponentMultigateway,
				metadata.LabelAppInstance:  clusterName,
			},
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "pg-replica",
					Port:       5433,
					TargetPort: intstr.FromString("pg-replica"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	require.NoError(t, watcher.WaitForMatch(expectedClusterIPSvc),
		"global multigateway Service should revert to ClusterIP after disabling")
	require.NoError(t, watcher.WaitForMatch(expectedReplicaClusterIPSvc),
		"global multigateway replica Service should remain ClusterIP")

	// Step 8: Verify gateway status is nil and condition is removed.
	assert.Eventually(t, func() bool {
		var mgc multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgc); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(mgc.Status.Conditions, multigresv1alpha1.ConditionGatewayExternalReady)
		return mgc.Status.Gateway == nil && cond == nil
	}, testTimeout, pollInterval, "gateway status should be nil and condition removed after disabling")
}

func TestExternalGateway_NoReadyGatewaysTransition(t *testing.T) {
	t.Parallel()

	const clusterName = "gw-no-ready"

	k8sClient, _ := setupIntegration(t)

	// Step 1: Create cluster with externalGateway enabled.
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: testNamespace,
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate:  "default",
				CellTemplate:  "default",
				ShardTemplate: "default",
			},
			Cells: []multigresv1alpha1.CellConfig{
				{Name: "zone-a", ZoneID: "use1-az1"},
			},
			ExternalGateway: &multigresv1alpha1.ExternalGatewayConfig{
				Enabled:     true,
				ExternalIPs: []multigresv1alpha1.IPAddress{"2001:db8::101"},
			},
		},
	}

	setTestPostgresPasswordSecretRef(cluster)

	require.NoError(t, k8sClient.Create(t.Context(), cluster))

	// Step 2: Wait for initial NoReadyGateways condition (external endpoint assigned, 0 ready gateways).
	assert.Eventually(t, func() bool {
		var mgc multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgc); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(mgc.Status.Conditions, multigresv1alpha1.ConditionGatewayExternalReady)
		return cond != nil &&
			cond.Status == metav1.ConditionFalse &&
			cond.Reason == multigresv1alpha1.ReasonNoReadyGateways &&
			strings.Contains(cond.Message, "no multigateway pods are ready") &&
			mgc.Status.Gateway != nil &&
			mgc.Status.Gateway.ExternalEndpoint == "2001:db8::101"
	}, testTimeout, pollInterval, "condition should be False/NoReadyGateways with endpoint populated")

	// Step 3: Simulate Cell reporting gatewayReadyReplicas > 0.
	var cellList multigresv1alpha1.CellList
	require.NoError(t, k8sClient.List(t.Context(), &cellList,
		client.InNamespace(testNamespace),
		client.MatchingLabels{"multigres.com/cluster": clusterName},
	))
	require.NotEmpty(t, cellList.Items, "expected at least one Cell CR")

	cell := &cellList.Items[0]
	cell.Status.GatewayReadyReplicas = 2
	cell.Status.GatewayReplicas = 2
	cell.Status.ObservedGeneration = cell.Generation
	require.NoError(t, k8sClient.Status().Update(t.Context(), cell))

	// Step 4: Verify condition transitions to True/EndpointReady.
	assert.Eventually(t, func() bool {
		var mgc multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgc); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(mgc.Status.Conditions, multigresv1alpha1.ConditionGatewayExternalReady)
		return cond != nil &&
			cond.Status == metav1.ConditionTrue &&
			cond.Reason == multigresv1alpha1.ReasonEndpointReady &&
			strings.Contains(cond.Message, "is serving traffic") &&
			mgc.Status.Gateway != nil &&
			mgc.Status.Gateway.ExternalEndpoint == "2001:db8::101"
	}, testTimeout, pollInterval, "condition should transition to True/EndpointReady after Cell reports ready gateways")

	// Verify observedGeneration is set correctly.
	var mgcCheck multigresv1alpha1.MultigresCluster
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgcCheck))
	cond := meta.FindStatusCondition(mgcCheck.Status.Conditions, multigresv1alpha1.ConditionGatewayExternalReady)
	require.NotNil(t, cond)
	assert.Equal(t, mgcCheck.Generation, cond.ObservedGeneration,
		"observedGeneration should match cluster generation")
}
