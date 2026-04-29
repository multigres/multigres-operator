//go:build integration
// +build integration

package multigrescluster_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestExternalAdminWeb_EnableDisableLifecycle(t *testing.T) {
	t.Parallel()

	const clusterName = "aw-lifecycle"

	k8sClient, watcher := setupIntegration(t)

	// Step 1: Create cluster with externalAdminWeb enabled and annotations.
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
			ExternalAdminWeb: &multigresv1alpha1.ExternalAdminWebConfig{
				Enabled:     true,
				ExternalIPs: []multigresv1alpha1.IPAddress{"2001:db8::200"},
				Annotations: map[string]string{
					"team.example.com/owner": "platform-engineering",
				},
			},
		},
	}

	require.NoError(t, k8sClient.Create(t.Context(), cluster))

	watcher.SetCmpOpts(
		testutil.IgnoreMetaRuntimeFields(),
		testutil.IgnoreServiceRuntimeFields(),
		cmpopts.IgnoreFields(corev1.ServiceSpec{}, "ExternalTrafficPolicy"),
		cmpopts.IgnoreFields(corev1.ServicePort{}, "NodePort"),
	)

	// Step 2: Verify the admin-web Service is created as ClusterIP with externalIPs + annotations.
	awLabels := clusterLabels(t, clusterName, "multiadmin-web", "")
	expectedAWSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName + "-multiadmin-web",
			Namespace: testNamespace,
			Labels:    awLabels,
			Annotations: map[string]string{
				"team.example.com/owner": "platform-engineering",
			},
			OwnerReferences: clusterOwnerRefs(t, clusterName),
		},
		Spec: corev1.ServiceSpec{
			Selector:    metadata.GetSelectorLabels(awLabels),
			Type:        corev1.ServiceTypeClusterIP,
			ExternalIPs: []string{"2001:db8::200"},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       18100,
					TargetPort: intstr.FromInt(18100),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	require.NoError(t, watcher.WaitForMatch(expectedAWSvc),
		"multiadmin-web Service should be ClusterIP with externalIPs and annotations")

	// Step 3: Verify initial condition is NoReadyAdminWeb (endpoint assigned via externalIP, 0 ready pods).
	assert.Eventually(t, func() bool {
		var mgc multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgc); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(mgc.Status.Conditions, multigresv1alpha1.ConditionAdminWebExternalReady)
		return cond != nil &&
			cond.Status == metav1.ConditionFalse &&
			cond.Reason == multigresv1alpha1.ReasonNoReadyAdminWeb &&
			mgc.Status.AdminWeb != nil &&
			mgc.Status.AdminWeb.ExternalEndpoint == "2001:db8::200"
	}, testTimeout, pollInterval, "condition should be False/NoReadyAdminWeb before pods are ready")

	// Step 4: Simulate the admin-web Deployment reporting ready replicas.
	var awDeploy appsv1.Deployment
	assert.Eventually(t, func() bool {
		err := k8sClient.Get(t.Context(), client.ObjectKey{
			Name:      clusterName + "-multiadmin-web",
			Namespace: testNamespace,
		}, &awDeploy)
		return err == nil
	}, testTimeout, pollInterval, "admin-web Deployment should exist")

	awDeploy.Status.ReadyReplicas = 1
	awDeploy.Status.Replicas = 1
	require.NoError(t, k8sClient.Status().Update(t.Context(), &awDeploy),
		"simulating admin-web Deployment reporting ready replicas")

	// Step 5: Verify condition transitions to EndpointReady.
	assert.Eventually(t, func() bool {
		var mgc multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgc); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(mgc.Status.Conditions, multigresv1alpha1.ConditionAdminWebExternalReady)
		return cond != nil &&
			cond.Status == metav1.ConditionTrue &&
			cond.Reason == multigresv1alpha1.ReasonEndpointReady &&
			mgc.Status.AdminWeb != nil &&
			mgc.Status.AdminWeb.ExternalEndpoint == "2001:db8::200"
	}, testTimeout, pollInterval, "condition should be True/EndpointReady with endpoint in status")

	// Verify observedGeneration is set on the condition.
	var mgcCheck multigresv1alpha1.MultigresCluster
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgcCheck))
	cond := meta.FindStatusCondition(mgcCheck.Status.Conditions, multigresv1alpha1.ConditionAdminWebExternalReady)
	require.NotNil(t, cond)
	assert.Equal(t, mgcCheck.Generation, cond.ObservedGeneration,
		"observedGeneration should match cluster generation")

	// Step 6: Disable external admin web and verify reversion.
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
	cluster.Spec.ExternalAdminWeb = &multigresv1alpha1.ExternalAdminWebConfig{
		Enabled: false,
	}
	require.NoError(t, k8sClient.Update(t.Context(), cluster),
		"disabling external admin web")

	// Step 7: Verify Service reverts to ClusterIP with no annotations.
	expectedClusterIPSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            clusterName + "-multiadmin-web",
			Namespace:       testNamespace,
			Labels:          awLabels,
			OwnerReferences: clusterOwnerRefs(t, clusterName),
		},
		Spec: corev1.ServiceSpec{
			Selector: metadata.GetSelectorLabels(awLabels),
			Type:     corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       18100,
					TargetPort: intstr.FromInt(18100),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	require.NoError(t, watcher.WaitForMatch(expectedClusterIPSvc),
		"multiadmin-web Service should revert to ClusterIP after disabling")

	// Step 8: Verify admin web status is nil and condition is removed.
	assert.Eventually(t, func() bool {
		var mgc multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgc); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(mgc.Status.Conditions, multigresv1alpha1.ConditionAdminWebExternalReady)
		return mgc.Status.AdminWeb == nil && cond == nil
	}, testTimeout, pollInterval, "admin web status should be nil and condition removed after disabling")
}

func TestExternalAdminWeb_NoReadyAdminWebTransition(t *testing.T) {
	t.Parallel()

	const clusterName = "aw-no-ready"

	k8sClient, _ := setupIntegration(t)

	// Step 1: Create cluster with externalAdminWeb enabled.
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
			ExternalAdminWeb: &multigresv1alpha1.ExternalAdminWebConfig{
				Enabled:     true,
				ExternalIPs: []multigresv1alpha1.IPAddress{"2001:db8::201"},
			},
		},
	}

	require.NoError(t, k8sClient.Create(t.Context(), cluster))

	// Step 2: Wait for initial NoReadyAdminWeb condition.
	assert.Eventually(t, func() bool {
		var mgc multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgc); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(mgc.Status.Conditions, multigresv1alpha1.ConditionAdminWebExternalReady)
		return cond != nil &&
			cond.Status == metav1.ConditionFalse &&
			cond.Reason == multigresv1alpha1.ReasonNoReadyAdminWeb &&
			strings.Contains(cond.Message, "no multiadmin-web pods are ready") &&
			mgc.Status.AdminWeb != nil &&
			mgc.Status.AdminWeb.ExternalEndpoint == "2001:db8::201"
	}, testTimeout, pollInterval, "condition should be False/NoReadyAdminWeb with endpoint populated")

	// Step 3: Simulate the admin-web Deployment reporting ready replicas.
	var awDeploy appsv1.Deployment
	assert.Eventually(t, func() bool {
		err := k8sClient.Get(t.Context(), client.ObjectKey{
			Name:      clusterName + "-multiadmin-web",
			Namespace: testNamespace,
		}, &awDeploy)
		return err == nil
	}, testTimeout, pollInterval, "admin-web Deployment should exist")

	awDeploy.Status.ReadyReplicas = 2
	awDeploy.Status.Replicas = 2
	require.NoError(t, k8sClient.Status().Update(t.Context(), &awDeploy))

	// Step 4: Verify condition transitions to True/EndpointReady.
	assert.Eventually(t, func() bool {
		var mgc multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgc); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(mgc.Status.Conditions, multigresv1alpha1.ConditionAdminWebExternalReady)
		return cond != nil &&
			cond.Status == metav1.ConditionTrue &&
			cond.Reason == multigresv1alpha1.ReasonEndpointReady &&
			strings.Contains(cond.Message, "is serving traffic") &&
			mgc.Status.AdminWeb != nil &&
			mgc.Status.AdminWeb.ExternalEndpoint == "2001:db8::201"
	}, testTimeout, pollInterval, "condition should transition to True/EndpointReady after Deployment reports ready")

	// Verify observedGeneration is set correctly.
	var mgcCheck multigresv1alpha1.MultigresCluster
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), &mgcCheck))
	cond := meta.FindStatusCondition(mgcCheck.Status.Conditions, multigresv1alpha1.ConditionAdminWebExternalReady)
	require.NotNil(t, cond)
	assert.Equal(t, mgcCheck.Generation, cond.ObservedGeneration,
		"observedGeneration should match cluster generation")
}
