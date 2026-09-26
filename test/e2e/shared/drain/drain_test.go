//go:build e2e

package drain_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/test/e2e/framework"

	"github.com/multigres/testkit/assert"
)

var cluster *framework.Cluster

func TestMain(m *testing.M) {
	var err error
	cluster, err = framework.EnsureSharedCluster()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e setup: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// TestExternalPoolerDeletion verifies recovery after replica and primary deletion.
func TestExternalPoolerDeletion(t *testing.T) {
	ck := assert.NewAborting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.NoError(err, "create CR client")
	ctx := context.Background()

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	cr.Name = "external-pod-delete"
	replicas := int32(3)
	pool := cr.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"]
	pool.ReplicasPerCell = &replicas
	cr.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"] = pool
	ck.NoError(c.Create(ctx, cr), "create MultigresCluster")

	t.Log("waiting for the initial three-pooler shard to become healthy")
	cluster.WaitForAllPodsReady(t, ns)
	shard := waitForHealthyShard(t, c, ns, cr.Name)
	primary, replica := waitForPrimaryAndReplica(t, c, ns, cr.Name, shard)

	t.Logf("deleting replica %q", replica.Name)
	deleteAndWaitForReplacement(t, c, replica)
	shard = waitForHealthyShard(t, c, ns, cr.Name)
	primaryAfterReplica, _ := waitForPrimaryAndReplica(t, c, ns, cr.Name, shard)
	ck.Eq(primary.Name, primaryAfterReplica.Name, "replica deletion changed primary from")

	t.Logf("deleting primary %q", primaryAfterReplica.Name)
	deleteAndWaitForReplacement(t, c, primaryAfterReplica)
	shard = waitForHealthyShard(t, c, ns, cr.Name)
	primaryAfterFailover, _ := waitForPrimaryAndReplica(t, c, ns, cr.Name, shard)
	ck.NotEq(primaryAfterReplica.Name, primaryAfterFailover.Name, "primary")
	t.Logf("failover completed with primary %q", primaryAfterFailover.Name)
}

// TestGracefulScaleDown verifies that a pool can scale down while remaining healthy.
func TestGracefulScaleDown(t *testing.T) {
	ck := assert.NewAborting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.NoError(err, "create CR client")
	ctx := context.Background()

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	cr.Name = "graceful-scale-down"
	replicas := int32(3)
	pool := cr.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"]
	pool.ReplicasPerCell = &replicas
	cr.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"] = pool
	ck.NoError(c.Create(ctx, cr), "create MultigresCluster")

	t.Log("waiting for the initial three-pooler shard to become healthy")
	cluster.WaitForAllPodsReady(t, ns)
	shard := waitForHealthyShard(t, c, ns, cr.Name)
	primary, _ := waitForPrimaryAndReplica(t, c, ns, cr.Name, shard)
	poolPodsBeforeScaleDown := listPoolPods(t, c, ns, cr.Name)

	t.Log("scaling the pool from three poolers to two")
	ck.NoError(c.Get(ctx, client.ObjectKeyFromObject(cr), cr), "get MultigresCluster")
	replicas = 2
	pool = cr.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"]
	pool.ReplicasPerCell = &replicas
	cr.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"] = pool
	ck.NoError(c.Update(ctx, cr), "scale down MultigresCluster")

	waitForPoolPodCount(t, c, ns, cr.Name, 2)
	shard = waitForHealthyShard(t, c, ns, cr.Name)
	primaryAfterScaleDown, _ := waitForPrimaryAndReplica(t, c, ns, cr.Name, shard)
	ck.Eq(primary.Name, primaryAfterScaleDown.Name, "scale-down changed primary from")
	removed := removedPoolPod(t, poolPodsBeforeScaleDown, listPoolPods(t, c, ns, cr.Name))
	probe := newMultiadminProbe(t, c, ns, cr.Name, primaryAfterScaleDown)
	t.Cleanup(func() { _ = c.Delete(context.Background(), probe) })
	waitForPoolerShutdown(t, ns, probe.Name, removed)
	waitForCohortRemoval(t, ns, probe.Name, removed)
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
	err := wait.PollUntilContextCancel(
		ctx,
		3*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
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
		},
	)
	assert.NewAborting(t).NoError(err, "timed out waiting for healthy shard")
	return found
}

func waitForPrimaryAndReplica(
	t testing.TB,
	c client.Client,
	namespace, clusterName string,
	shard *multigresv1alpha1.Shard,
) (*corev1.Pod, *corev1.Pod) {
	t.Helper()
	var primary, replica *corev1.Pod
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(
		ctx,
		3*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			freshShard := &multigresv1alpha1.Shard{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(shard), freshShard); err != nil {
				return false, nil
			}
			pods := &corev1.PodList{}
			if err := c.List(ctx, pods,
				client.InNamespace(namespace),
				client.MatchingLabels{
					metadata.LabelMultigresCluster: clusterName,
					metadata.LabelMultigresPool:    "default",
				},
			); err != nil {
				return false, nil
			}
			primary, replica = nil, nil
			for i := range pods.Items {
				pod := &pods.Items[i]
				if !podReady(pod) {
					return false, nil
				}
				switch freshShard.Status.PodRoles[pod.Name] {
				case "PRIMARY":
					primary = pod.DeepCopy()
				case "REPLICA":
					if replica == nil {
						replica = pod.DeepCopy()
					}
				}
			}
			return primary != nil && replica != nil, nil
		},
	)
	assert.NewAborting(t).NoError(err, "timed out waiting for primary and replica")
	return primary, replica
}

func deleteAndWaitForReplacement(t testing.TB, c client.Client, pod *corev1.Pod) {
	t.Helper()
	ck := assert.NewAborting(t)
	ctx := context.Background()
	ck.NoError(c.Delete(ctx, pod), "delete pod %q", pod.Name)

	key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(
		waitCtx,
		3*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			replacement := &corev1.Pod{}
			if err := c.Get(ctx, key, replacement); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			return replacement.UID != pod.UID && replacement.DeletionTimestamp.IsZero() &&
				podReady(replacement), nil
		},
	)
	ck.NoError(err, "timed out waiting for replacement of pod %q", pod.Name)
}

func waitForPoolPodCount(t testing.TB, c client.Client, namespace, clusterName string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(
		ctx,
		3*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			pods := &corev1.PodList{}
			if err := c.List(ctx, pods,
				client.InNamespace(namespace),
				client.MatchingLabels{
					metadata.LabelMultigresCluster: clusterName,
					metadata.LabelMultigresPool:    "default",
				},
			); err != nil {
				return false, err
			}
			if len(pods.Items) != want {
				return false, nil
			}
			for i := range pods.Items {
				if !pods.Items[i].DeletionTimestamp.IsZero() || !podReady(&pods.Items[i]) {
					return false, nil
				}
			}
			return true, nil
		},
	)
	assert.NewAborting(t).NoError(err, "timed out waiting for %d ready pool pods", want)
}

func listPoolPods(t testing.TB, c client.Client, namespace, clusterName string) []*corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	assert.NewAborting(t).NoError(c.List(
		context.Background(),
		pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			metadata.LabelMultigresCluster: clusterName,
			metadata.LabelMultigresPool:    "default",
		},
	), "list pool pods")
	result := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		result = append(result, pods.Items[i].DeepCopy())
	}
	return result
}

func removedPoolPod(t testing.TB, before, after []*corev1.Pod) *corev1.Pod {
	t.Helper()
	present := make(map[string]struct{}, len(after))
	for _, pod := range after {
		present[pod.Name] = struct{}{}
	}

	var removed []*corev1.Pod
	for _, pod := range before {
		if _, ok := present[pod.Name]; !ok {
			removed = append(removed, pod)
		}
	}
	assert.NewAborting(t).Len(removed, 1, "removed pool pods = %d, want 1", len(removed))
	return removed[0]
}

func waitForCohortRemoval(
	t testing.TB,
	namespace, probeName string,
	removed *corev1.Pod,
) {
	t.Helper()
	removedID := poolerServiceID(t, removed)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(
		ctx,
		2*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			status, err := cohortProbeStatus(ctx, namespace, probeName)
			if err != nil {
				return false, nil
			}
			members := status.GetConsensusStatus().
				GetCurrentPosition().
				GetPosition().
				GetDecision().
				GetCohortMembers()
			if len(members) != 2 {
				return false, nil
			}
			for _, member := range members {
				if member.GetName() == removedID {
					return false, nil
				}
			}
			return true, nil
		},
	)
	assert.NewAborting(t).NoError(err, "cohort still contains removed pooler %q", removedID)
}

func waitForPoolerShutdown(t testing.TB, namespace, probeName string, removed *corev1.Pod) {
	t.Helper()
	removedID := poolerServiceID(t, removed)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(
		ctx,
		2*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			poolers, err := poolersProbeStatus(ctx, namespace, probeName)
			if err != nil {
				return false, nil
			}
			for _, pooler := range poolers.GetPoolers() {
				if pooler.GetId().GetName() == removedID {
					return pooler.GetLifecycleStatus().
						GetStatus() ==
						clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN, nil
				}
			}
			return false, nil
		},
	)
	assert.NewAborting(t).NoError(err, "pooler %q did not reach LIFECYCLE_SHUTDOWN", removedID)
}

func newMultiadminProbe(
	t testing.TB,
	c client.Client,
	namespace, clusterName string,
	primary *corev1.Pod,
) *corev1.Pod {
	t.Helper()
	image := multiadminImage(t, c, namespace, clusterName)
	cell := primary.Labels[metadata.LabelMultigresCell]
	primaryID := poolerServiceID(t, primary)
	body := fmt.Sprintf(`{"poolerId":{"cell":%q,"name":%q}}`, cell, primaryID)
	host := clusterName + "-multiadmin"
	script := fmt.Sprintf(`body='%s'
request() {
  local method=$1 path=$2 payload=$3
  exec 3<>/dev/tcp/%s/18000 || return
  if [ -n "$payload" ]; then
    printf '%%s %%s HTTP/1.0\r\nHost: %s\r\nContent-Type: application/json\r\nConnect-Protocol-Version: 1\r\nContent-Length: %%s\r\n\r\n%%s' "$method" "$path" "${#payload}" "$payload" >&3
  else
    printf '%%s %%s HTTP/1.0\r\nHost: %s\r\n\r\n' "$method" "$path" >&3
  fi
  cat <&3
  exec 3>&- 3<&-
}
while true; do
  echo __POOLERS_PROBE__
  request GET /api/v1/poolers ''
  echo __END_POOLERS_PROBE__
  echo __COHORT_PROBE__
  request POST /multiadmin.MultiadminService/GetPoolerStatus "$body"
  echo __END_COHORT_PROBE__
  sleep 2
done`, body, host, host, host)
	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "cohort-probe", Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "probe",
				Image:   image,
				Command: []string{"/bin/bash", "-c", script},
			}},
		},
	}
	assert.NewAborting(t).NoError(c.Create(context.Background(), probe), "create cohort probe")
	return probe
}

func multiadminImage(t testing.TB, c client.Client, namespace, clusterName string) string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(
		context.Background(),
		pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			metadata.LabelAppComponent:     metadata.ComponentMultiadmin,
			metadata.LabelMultigresCluster: clusterName,
		},
	); err != nil || len(pods.Items) != 1 || len(pods.Items[0].Spec.Containers) != 1 {
		t.Fatalf("get multiadmin pod image: %v", err)
	}
	return pods.Items[0].Spec.Containers[0].Image
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

func cohortProbeStatus(
	ctx context.Context,
	namespace, podName string,
) (*multiadminpb.GetPoolerStatusResponse, error) {
	output, err := multiadminProbeOutput(ctx, namespace, podName)
	if err != nil {
		return nil, err
	}
	body, err := parseProbeResponse(output, "__COHORT_PROBE__", "__END_COHORT_PROBE__")
	if err != nil {
		return nil, err
	}
	status := &multiadminpb.GetPoolerStatusResponse{}
	if err := protojson.Unmarshal(body, status); err != nil {
		return nil, err
	}
	return status, nil
}

func poolersProbeStatus(
	ctx context.Context,
	namespace, podName string,
) (*multiadminpb.GetPoolersResponse, error) {
	output, err := multiadminProbeOutput(ctx, namespace, podName)
	if err != nil {
		return nil, err
	}
	body, err := parseProbeResponse(output, "__POOLERS_PROBE__", "__END_POOLERS_PROBE__")
	if err != nil {
		return nil, err
	}
	poolers := &multiadminpb.GetPoolersResponse{}
	if err := protojson.Unmarshal(body, poolers); err != nil {
		return nil, err
	}
	return poolers, nil
}

func multiadminProbeOutput(ctx context.Context, namespace, podName string) (string, error) {
	stream, err := cluster.Clientset.CoreV1().
		Pods(namespace).
		GetLogs(podName, &corev1.PodLogOptions{}).
		Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	output, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func parseProbeResponse(output, startMarker, endMarker string) ([]byte, error) {
	responses := strings.Split(output, endMarker)
	for i := len(responses) - 1; i >= 0; i-- {
		response := responses[i]
		start := strings.LastIndex(response, startMarker)
		if start == -1 {
			continue
		}
		httpResponse, err := http.ReadResponse(
			bufio.NewReader(
				strings.NewReader(strings.TrimSpace(response[start+len(startMarker):])),
			),
			&http.Request{Method: http.MethodPost},
		)
		if err != nil {
			continue
		}
		body, err := io.ReadAll(httpResponse.Body)
		_ = httpResponse.Body.Close()
		if err != nil || httpResponse.StatusCode != http.StatusOK {
			continue
		}
		return body, nil
	}
	return nil, fmt.Errorf("no valid %s response", startMarker)
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
