//go:build e2e

package rulechange_test

import (
	"context"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/test/e2e/framework"
)

// TestApplyCertifiedRuleChange is a regression test for a bug where
// multiadmin could not reach multiorch: multiorch advertised a pod-local
// hostname (via failed hostname discovery) that was unresolvable from
// multiadmin's pod. It exercises ApplyCertifiedRuleChange, the one RPC path
// where multiadmin dials multiorch directly using multiorch's
// self-registered topology hostname, by re-proposing the shard's current
// (already-installed) leader/cohort with --unsafe-derive-cert-from-reachable
// so no external cert material has to be computed by the test.
//
// The CLI runs from inside the multiadmin pod via kubectl exec, dialing
// "localhost" for the admin server, so only the multiadmin -> multiorch hop
// crosses real cluster DNS -- the hop that was broken.
func TestApplyCertifiedRuleChange(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}
	ctx := context.Background()

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	cr.Name = "rule-change"
	if err := c.Create(ctx, cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}

	t.Log("waiting for the shard to become healthy")
	cluster.WaitForAllPodsReady(t, ns)
	shard := waitForHealthyShard(t, c, ns, cr.Name)

	leader, cohort := currentLeaderAndCohort(t, c, ns, cr.Name, shard)
	multiadminPod := multiadminPodName(t, c, ns, cr.Name)

	args := []string{
		"--kubeconfig", cluster.Kubeconfig,
		"exec", "-n", ns, multiadminPod, "--",
		"multigres", "cluster", "apply-rule-change",
		"--admin-server=localhost:18070",
		"--database=" + string(shard.Spec.DatabaseName),
		"--table-group=" + string(shard.Spec.TableGroupName),
		"--shard=" + string(shard.Spec.ShardName),
		"--leader=" + leader,
		"--cohort=" + strings.Join(cohort, ","),
		"--durability=" + shard.Spec.DurabilityPolicy,
		"--unsafe-derive-cert-from-reachable",
		"--yes",
		"--reason=e2e regression test for multiadmin->multiorch hostname resolution",
	}
	execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(execCtx, "kubectl", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("apply-rule-change failed: %v: %s", err, strings.TrimSpace(string(out)))
	}
	t.Logf("apply-rule-change output: %s", strings.TrimSpace(string(out)))

	// Confirm multiorch actually installed the proposed rule rather than the
	// CLI merely exiting 0 on some other no-op path: the reported leader must
	// match what we proposed, and the term must have advanced past the
	// shard's prior rule (term=1, installed by the operator's own bootstrap).
	installedTerm, installedLeader := parseInstalledRule(t, string(out))
	if installedLeader != leader {
		t.Fatalf("installed rule leader = %q, want %q", installedLeader, leader)
	}
	if installedTerm <= 1 {
		t.Fatalf("installed rule term = %d, want > 1 (a genuinely new rule, not the initial appointment)", installedTerm)
	}
}

var installedRuleRE = regexp.MustCompile(`Installed rule term=(\d+), leader=(\S+)/(\S+)`)

// parseInstalledRule extracts the term and "cell_name" leader encoding from
// apply-rule-change's stdout ("Installed rule term=%d, leader=%s/%s").
func parseInstalledRule(t testing.TB, out string) (term int, leader string) {
	t.Helper()
	m := installedRuleRE.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("apply-rule-change did not report an installed rule: %s", out)
	}
	term, err := strconv.Atoi(m[1])
	if err != nil {
		t.Fatalf("parse installed rule term %q: %v", m[1], err)
	}
	return term, m[2] + "_" + m[3]
}

func waitForHealthyShard(
	t testing.TB,
	c client.Client,
	namespace, clusterName string,
) *multigresv1alpha1.Shard {
	t.Helper()
	var found *multigresv1alpha1.Shard
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		shards := &multigresv1alpha1.ShardList{}
		if err := c.List(ctx, shards,
			client.InNamespace(namespace),
			client.MatchingLabels{metadata.LabelMultigresCluster: clusterName},
		); err != nil || len(shards.Items) != 1 {
			return false, nil
		}
		shard := &shards.Items[0]
		if shard.Status.Phase != multigresv1alpha1.PhaseHealthy || !shard.Status.OrchReady {
			return false, nil
		}
		found = shard
		return true, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for healthy shard: %v", err)
	}
	return found
}

// currentLeaderAndCohort returns the shard's already-installed leader and
// full cohort, encoded as "cell_name" pooler IDs (the format
// apply-rule-change's --leader/--cohort flags expect).
func currentLeaderAndCohort(
	t testing.TB,
	c client.Client,
	namespace, clusterName string,
	shard *multigresv1alpha1.Shard,
) (leader string, cohort []string) {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			metadata.LabelMultigresCluster: clusterName,
			metadata.LabelMultigresPool:    "default",
		},
	); err != nil {
		t.Fatalf("list pool pods: %v", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		cell := pod.Labels[metadata.LabelMultigresCell]
		id := poolerServiceID(t, pod)
		clusterID := cell + "_" + id
		cohort = append(cohort, clusterID)
		if shard.Status.PodRoles[pod.Name] == "PRIMARY" {
			leader = clusterID
		}
	}
	if leader == "" {
		t.Fatalf("no PRIMARY pod found among pool pods")
	}
	if len(cohort) == 0 {
		t.Fatalf("no pool pods found")
	}
	return leader, cohort
}

func poolerServiceID(t testing.TB, pod *corev1.Pod) string {
	t.Helper()
	for _, container := range pod.Spec.Containers {
		for _, arg := range container.Args {
			if serviceID, ok := strings.CutPrefix(arg, "--service-id="); ok {
				return serviceID
			}
		}
	}
	t.Fatalf("pooler pod %q has no service ID", pod.Name)
	return ""
}

func multiadminPodName(t testing.TB, c client.Client, namespace, clusterName string) string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			metadata.LabelAppComponent:     metadata.ComponentMultiadmin,
			metadata.LabelMultigresCluster: clusterName,
		},
	); err != nil || len(pods.Items) != 1 {
		t.Fatalf("get multiadmin pod: %v", err)
	}
	return pods.Items[0].Name
}
