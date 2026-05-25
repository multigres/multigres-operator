//go:build e2e

package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// Namespace
// ---------------------------------------------------------------------------

// CreateNamespace creates an isolated namespace and registers t.Cleanup.
func (c *Cluster) CreateNamespace(t testing.TB) string {
	t.Helper()
	ns := envconf.RandomName("e2e-ns", 16)
	_, err := c.Clientset.CoreV1().Namespaces().Create(
		context.Background(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	_, err = c.Clientset.CoreV1().Secrets(ns).Create(
		context.Background(),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "multigres-admin-password"},
			Type:       corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"password": []byte("postgres"),
			},
		},
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatalf("create postgres password secret: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Clientset.CoreV1().Namespaces().Delete(
			context.Background(), ns, metav1.DeleteOptions{})
	})
	return ns
}

// ---------------------------------------------------------------------------
// CI Resources
// ---------------------------------------------------------------------------

// CIResources returns minimal resource requests suitable for CI runners
// (10m CPU, 32Mi memory).
func CIResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("32Mi"),
		},
	}
}

// CIContainerConfig returns a ContainerConfig with minimal resource requests.
func CIContainerConfig() multigresv1alpha1.ContainerConfig {
	return multigresv1alpha1.ContainerConfig{Resources: CIResources()}
}

// WithCIResources applies minimal resource requests to a MultigresClusterSpec.
// It pre-populates the default database structure (mirroring the operator's
// resolver) so resource overrides are in place before the CR is submitted.
// Does NOT change replica counts or deployment topology.
func WithCIResources(spec *multigresv1alpha1.MultigresClusterSpec) {
	// Etcd
	if spec.GlobalTopoServer == nil {
		spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{}
	}
	if spec.GlobalTopoServer.Etcd == nil {
		spec.GlobalTopoServer.Etcd = &multigresv1alpha1.EtcdSpec{}
	}
	spec.GlobalTopoServer.Etcd.Resources = CIResources()

	// MultiAdmin
	if spec.MultiAdmin == nil {
		spec.MultiAdmin = &multigresv1alpha1.MultiAdminConfig{}
	}
	if spec.MultiAdmin.Spec == nil {
		spec.MultiAdmin.Spec = &multigresv1alpha1.StatelessSpec{}
	}
	spec.MultiAdmin.Spec.Resources = CIResources()

	// MultiAdmin Web
	if spec.MultiAdminWeb == nil {
		spec.MultiAdminWeb = &multigresv1alpha1.MultiAdminWebConfig{}
	}
	if spec.MultiAdminWeb.Spec == nil {
		spec.MultiAdminWeb.Spec = &multigresv1alpha1.StatelessSpec{}
	}
	spec.MultiAdminWeb.Spec.Resources = CIResources()

	// Cells → gateway
	for i := range spec.Cells {
		if spec.Cells[i].Spec == nil {
			spec.Cells[i].Spec = &multigresv1alpha1.CellInlineSpec{}
		}
		spec.Cells[i].Spec.MultiGateway.Resources = CIResources()
	}

	// Databases → shards → multiorch, pools
	if len(spec.Databases) == 0 {
		spec.Databases = []multigresv1alpha1.DatabaseConfig{{
			Name: "postgres", Default: true,
		}}
	}
	for i := range spec.Databases {
		if len(spec.Databases[i].TableGroups) == 0 {
			spec.Databases[i].TableGroups = []multigresv1alpha1.TableGroupConfig{{
				Name: "default", Default: true,
			}}
		}
		for j := range spec.Databases[i].TableGroups {
			if len(spec.Databases[i].TableGroups[j].Shards) == 0 {
				spec.Databases[i].TableGroups[j].Shards = []multigresv1alpha1.ShardConfig{{
					Name: "0-inf",
					Spec: &multigresv1alpha1.ShardInlineSpec{
						Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
							"default": {Type: "readWrite"},
						},
					},
				}}
			}
			for k := range spec.Databases[i].TableGroups[j].Shards {
				shard := &spec.Databases[i].TableGroups[j].Shards[k]
				// Shards with overrides get resources from their template —
				// setting Spec here would violate the "spec XOR overrides" rule.
				if shard.Overrides != nil {
					continue
				}
				if shard.Spec == nil {
					shard.Spec = &multigresv1alpha1.ShardInlineSpec{}
				}
				shard.Spec.MultiOrch.Resources = CIResources()
				if shard.Spec.Pools == nil {
					shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{}
				}
				for name, pool := range shard.Spec.Pools {
					pool.Postgres = CIContainerConfig()
					pool.Multipooler = CIContainerConfig()
					shard.Spec.Pools[name] = pool
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Wait helpers
// ---------------------------------------------------------------------------

// WaitForAllPodsReady waits until every pod in the namespace is Ready or
// Succeeded. Timeout 8 minutes.
func (c *Cluster) WaitForAllPodsReady(t testing.TB, ns string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	t.Logf("waiting for all pods in %s to be ready...", ns)
	err := wait.PollUntilContextCancel(ctx, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		pods, err := c.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, nil
		}
		if len(pods.Items) == 0 {
			return false, nil
		}
		for _, pod := range pods.Items {
			if pod.Status.Phase == corev1.PodSucceeded {
				continue
			}
			if pod.Status.Phase != corev1.PodRunning {
				return false, nil
			}
			ready := false
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					ready = true
					break
				}
			}
			if !ready {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		// Log pod states for debugging.
		pods, _ := c.Clientset.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
		if pods != nil {
			for _, pod := range pods.Items {
				ready := false
				for _, cond := range pod.Status.Conditions {
					if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
						ready = true
					}
				}
				t.Logf("  pod %s/%s phase=%s ready=%v", ns, pod.Name, pod.Status.Phase, ready)
			}
		}
		t.Fatalf("not all pods in %s ready: %v", ns, err)
	}
	t.Logf("all pods in %s ready ✓", ns)
}

// WaitForDeployment waits for a Deployment with the given container name.
func WaitForDeployment(t testing.TB, c client.Client, ns, containerName string) *appsv1.Deployment {
	t.Helper()
	var found *appsv1.Deployment
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		list := &appsv1.DeploymentList{}
		if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
			return false, nil
		}
		for i := range list.Items {
			for _, cont := range list.Items[i].Spec.Template.Spec.Containers {
				if cont.Name == containerName {
					found = &list.Items[i]
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for Deployment with container %q: %v", containerName, err)
	}
	return found
}

// WaitForStatefulSet waits for a StatefulSet with the given container name.
func WaitForStatefulSet(t testing.TB, c client.Client, ns, containerName string) *appsv1.StatefulSet {
	t.Helper()
	var found *appsv1.StatefulSet
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		list := &appsv1.StatefulSetList{}
		if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
			return false, nil
		}
		for i := range list.Items {
			for _, cont := range list.Items[i].Spec.Template.Spec.Containers {
				if cont.Name == containerName {
					found = &list.Items[i]
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for StatefulSet with container %q: %v", containerName, err)
	}
	return found
}

// WaitForPod waits for at least one Pod with the given container name.
func WaitForPod(t testing.TB, c client.Client, ns, containerName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		list := &corev1.PodList{}
		if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
			return false, nil
		}
		for _, pod := range list.Items {
			for _, cont := range pod.Spec.Containers {
				if cont.Name == containerName {
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for Pod with container %q: %v", containerName, err)
	}
}

// WaitForService waits for a Service with the given port name and number.
func WaitForService(t testing.TB, c client.Client, ns, portName string, port int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		list := &corev1.ServiceList{}
		if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
			return false, nil
		}
		for _, svc := range list.Items {
			for _, p := range svc.Spec.Ports {
				if p.Name == portName && p.Port == port {
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for Service %s:%d: %v", portName, port, err)
	}
}

// WaitForCRDCount waits until at least minCount items exist.
func WaitForCRDCount[T client.ObjectList](
	t testing.TB, c client.Client, ns string,
	list T, countFn func(T) int, minCount int, desc string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
			return false, nil
		}
		return countFn(list) >= minCount, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for %s (want >= %d): %v", desc, minCount, err)
	}
}

// WaitForEmpty waits until a list resource has zero items.
func WaitForEmpty[T client.ObjectList](
	t testing.TB, c client.Client, ns string,
	list T, countFn func(T) int, desc string, timeout time.Duration,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
			return false, nil
		}
		return countFn(list) == 0, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for %s to be empty: %v", desc, err)
	}
}

// ---------------------------------------------------------------------------
// Connectivity helpers
// ---------------------------------------------------------------------------

// FindGatewayService finds the multigateway Service with postgres:5432.
func FindGatewayService(t testing.TB, c *Cluster, ns string) string {
	t.Helper()
	var svcName string
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		svcs, err := c.Clientset.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, nil
		}
		for _, svc := range svcs.Items {
			for _, port := range svc.Spec.Ports {
				if port.Name == "postgres" && port.Port == 5432 {
					svcName = svc.Name
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("timed out finding multigateway service: %v", err)
	}
	return svcName
}

// WaitForQueryServing polls until SELECT 1 succeeds through the gateway.
func WaitForQueryServing(t testing.TB, c *Cluster, ns, gatewaySvc string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		pods, err := c.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, nil
		}
		var targetPod string
		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodRunning {
				continue
			}
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.Name == "postgres" && cs.Ready {
					targetPod = pod.Name
					break
				}
			}
			if targetPod != "" {
				break
			}
		}
		if targetPod == "" {
			return false, nil
		}
		args := []string{
			"--kubeconfig", c.Kubeconfig,
			"exec", "-n", ns, targetPod, "-c", "postgres", "--",
			"sh", "-c",
			fmt.Sprintf("PGPASSWORD=postgres psql -h %s -p 5432 -U postgres -d postgres -t -A -c 'SELECT 1'", gatewaySvc),
		}
		execCtx, execCancel := context.WithTimeout(ctx, 10*time.Second)
		defer execCancel()
		out, err := exec.CommandContext(execCtx, "kubectl", args...).CombinedOutput()
		if err != nil {
			t.Logf("psql attempt via %s/%s → %s:5432: %v: %s", ns, targetPod, gatewaySvc, err, strings.TrimSpace(string(out)))
			return false, nil
		}
		result := strings.TrimSpace(string(out))
		if result != "1" {
			t.Logf("psql unexpected result: %q", result)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for query serving via multigateway: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Mutation & assertion helpers
// ---------------------------------------------------------------------------

// WaitForPodCount waits until exactly count pods match the given labels.
func WaitForPodCount(t testing.TB, c client.Client, ns string, labels client.MatchingLabels, count int, desc string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		list := &corev1.PodList{}
		if err := c.List(ctx, list, client.InNamespace(ns), labels); err != nil {
			return false, nil
		}
		return len(list.Items) == count, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for %d %s pods: %v", count, desc, err)
	}
}

// WaitForDeploymentReplicas waits until a Deployment with the given container
// name has the expected number of ready replicas.
func WaitForDeploymentReplicas(t testing.TB, c client.Client, ns, containerName string, replicas int32) *appsv1.Deployment {
	t.Helper()
	var found *appsv1.Deployment
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		list := &appsv1.DeploymentList{}
		if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
			return false, nil
		}
		for i := range list.Items {
			for _, cont := range list.Items[i].Spec.Template.Spec.Containers {
				if cont.Name == containerName {
					found = &list.Items[i]
					return found.Status.ReadyReplicas == replicas, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for Deployment %q to have %d ready replicas: %v", containerName, replicas, err)
	}
	return found
}

// WaitForPodAnnotation waits until all pods matching labels have the given
// annotation key=value pair.
func WaitForPodAnnotation(t testing.TB, c client.Client, ns string, labels client.MatchingLabels, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		list := &corev1.PodList{}
		if err := c.List(ctx, list, client.InNamespace(ns), labels); err != nil {
			return false, nil
		}
		if len(list.Items) == 0 {
			return false, nil
		}
		for _, pod := range list.Items {
			if pod.Annotations[key] != value {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for annotation %s=%s on pods: %v", key, value, err)
	}
}

// WaitForPodToleration waits until all pods matching labels have a toleration
// with the given key.
func WaitForPodToleration(t testing.TB, c client.Client, ns string, labels client.MatchingLabels, tolerationKey string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		list := &corev1.PodList{}
		if err := c.List(ctx, list, client.InNamespace(ns), labels); err != nil {
			return false, nil
		}
		if len(list.Items) == 0 {
			return false, nil
		}
		for _, pod := range list.Items {
			found := false
			for _, tol := range pod.Spec.Tolerations {
				if tol.Key == tolerationKey {
					found = true
					break
				}
			}
			if !found {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for toleration %q on pods: %v", tolerationKey, err)
	}
}

// PatchCluster applies a JSON merge patch to a MultigresCluster and waits for
// the controller to observe the new generation.
func PatchCluster(t testing.TB, c client.Client, cr *multigresv1alpha1.MultigresCluster, patch []byte) {
	t.Helper()
	if err := c.Patch(context.Background(), cr, client.RawPatch(types.MergePatchType, patch)); err != nil {
		t.Fatalf("patch MultigresCluster: %v", err)
	}
	// Wait for the controller to observe the new generation.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	key := client.ObjectKeyFromObject(cr)
	err := wait.PollUntilContextCancel(ctx, 2*time.Second, false, func(ctx context.Context) (bool, error) {
		live := &multigresv1alpha1.MultigresCluster{}
		if err := c.Get(ctx, key, live); err != nil {
			return false, nil
		}
		return live.Status.ObservedGeneration >= live.Generation, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for observedGeneration to catch up: %v", err)
	}
}

// AssertWebhookRejects verifies that updating the given object fails with a
// webhook admission error containing the expected substring.
func AssertWebhookRejects(t testing.TB, c client.Client, obj client.Object, expectedMsg string) {
	t.Helper()
	err := c.Update(context.Background(), obj)
	if err == nil {
		t.Fatalf("expected webhook rejection containing %q, but update succeeded", expectedMsg)
	}
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Fatalf("expected webhook error containing %q, got: %v", expectedMsg, err)
	}
}

// ---------------------------------------------------------------------------
// Resource inspection helpers
// ---------------------------------------------------------------------------

// ListPDBs returns all PodDisruptionBudgets in the namespace.
func ListPDBs(t testing.TB, c client.Client, ns string) []policyv1.PodDisruptionBudget {
	t.Helper()
	list := &policyv1.PodDisruptionBudgetList{}
	if err := c.List(context.Background(), list, client.InNamespace(ns)); err != nil {
		t.Fatalf("list PDBs: %v", err)
	}
	return list.Items
}

// GetCluster fetches the live MultigresCluster by name from the namespace.
func GetCluster(t testing.TB, c client.Client, ns, name string) *multigresv1alpha1.MultigresCluster {
	t.Helper()
	cr := &multigresv1alpha1.MultigresCluster{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, cr); err != nil {
		t.Fatalf("get MultigresCluster %s/%s: %v", ns, name, err)
	}
	return cr
}

// MustMarshal JSON-encodes v and panics on error.
func MustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("json.Marshal: %v", err))
	}
	return data
}
