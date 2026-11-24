//go:build e2e

package tablegroups_test

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/test/e2e/framework"
)

const (
	database       = "postgres"
	tablegroup     = "lifecycle"
	createSQL      = "CREATE TABLEGROUP lifecycle SHARDS ('0-80', '80-inf')"
	catalogSQL     = "SELECT tablegroup_id, state FROM multigres.tablegroup WHERE name = 'lifecycle'"
	storageProbe   = "SELECT obj_description('public'::regnamespace, 'pg_namespace') IS NOT DISTINCT FROM 'first incarnation'"
	requestTimeout = 20 * time.Second
)

var ranges = []string{"0-80", "80-inf"}

func TestDynamicTablegroupLifecycle(t *testing.T) {
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	require.NoError(t, err)
	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	require.Empty(t, cr.Spec.Mode, "lifecycle requires the distributed fixture")
	images := recordPair(t, cr)
	cr.Annotations = map[string]string{multigresv1alpha1.AnnotationDynamicTableGroups: "true"}
	cr.Spec.PVCDeletionPolicy.WhenDeleted = multigresv1alpha1.RetainPVCRetentionPolicy
	cr.Spec.PVCDeletionPolicy.WhenScaled = multigresv1alpha1.RetainPVCRetentionPolicy

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	require.NoError(t, c.Create(ctx, cr), "create opted-in distributed cluster")
	cancel()
	cluster.WaitForAllPodsReady(t, ns)
	gw := framework.FindGatewayService(t, cluster, ns)
	framework.WaitForQueryServing(t, cluster, ns, gw)

	check := lifecycleCheck{client: c, namespace: ns, name: cr.Name, cell: string(cr.Spec.Cells[0].Name)}
	ctx, cancel = context.WithTimeout(t.Context(), requestTimeout)
	var defaultPods corev1.PodList
	err = c.List(ctx, &defaultPods, client.InNamespace(ns), client.MatchingLabels{
		metadata.LabelMultigresCluster: cr.Name, metadata.LabelMultigresDatabase: database,
		metadata.LabelMultigresTableGroup: "default", metadata.LabelMultigresPool: "default",
	})
	cancel()
	require.NoError(t, err, "list default pool pods for gateway SQL client")
	require.NotEmpty(t, defaultPods.Items, "default pool psql client")
	check.gateway = endpoint{namespace: ns, pod: defaultPods.Items[0].Name, host: gw}

	var topo multigresv1alpha1.TopoServer
	ctx, cancel = context.WithTimeout(t.Context(), requestTimeout)
	err = c.Get(ctx, client.ObjectKey{Namespace: ns, Name: check.topoName()}, &topo)
	cancel()
	require.NoError(t, err, "read the distributed cluster's global topology configuration")
	require.NotNil(t, topo.Spec.Etcd)
	require.NotEmpty(t, topo.Spec.Etcd.RootPath)
	check.topoRoot = topo.Spec.Etcd.RootPath

	check.gateway.query(t, "CREATE TABLE default_probe (marker text PRIMARY KEY, checks integer)")
	check.gateway.query(t, "INSERT INTO default_probe VALUES ('still serving', 0)")

	firstID, first := check.create(t)
	oldStorage := check.waitForStorage(t, firstID)
	check.requireRuntimeImages(t, images)
	check.requireClaims(t, firstID)
	// A schema comment persists on disk without adding a member table that could block DROP.
	for _, shard := range ranges {
		first[shard].query(t, "COMMENT ON SCHEMA public IS 'first incarnation'")
		require.Equal(t, "t", first[shard].query(t, storageProbe), "old data marker on shard %s", shard)
	}

	check.drop(t, firstID)
	check.requireRetainedStorage(t, oldStorage)
	check.gateway.checkDefault(t, 1)

	secondID, second := check.create(t)
	require.NotEqual(t, firstID, secondID, "recreated catalog ID")
	newStorage := check.waitForStorage(t, secondID)
	check.requireRuntimeImages(t, images)
	for claim, current := range newStorage {
		require.NotContains(t, oldStorage, claim, "recreated pool pod references retained PVC %s", claim)
		for oldClaim, old := range oldStorage {
			require.NotEqual(t, old.volume, current.volume, "new PVC %s reuses old PV %s of PVC %s", claim, old.volume, oldClaim)
		}
	}
	check.requireRetainedStorage(t, oldStorage)
	for _, shard := range ranges {
		require.Equal(t, "f", second[shard].query(t, storageProbe), "recreated shard %s inherited the old data marker", shard)
	}

	check.gateway.checkDefault(t, 2)
	t.Logf("lifecycle complete: catalog IDs %d and %d, retained PVCs %d, new PVCs %d", firstID, secondID, len(oldStorage), len(newStorage))
}

func recordPair(t *testing.T, cr *multigresv1alpha1.MultigresCluster) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()
	revision, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	require.NoError(t, err, "identify operator checkout")
	t.Logf("operator revision=%s operator image=%s", strings.TrimSpace(string(revision)), testutil.DefaultOperatorPreset().Image)
	images := cr.Spec.Images
	expected := make(map[string]string)
	for _, component := range []struct {
		name  string
		image multigresv1alpha1.ImageRef
	}{
		{"postgres", cmp.Or(images.Postgres, multigresv1alpha1.DefaultPostgresImage)},
		{"multiadmin", cmp.Or(images.Multiadmin, multigresv1alpha1.DefaultMultiadminImage)},
		{"multiadmin-web", cmp.Or(images.MultiadminWeb, multigresv1alpha1.DefaultMultiadminWebImage)},
		{"multiorch", cmp.Or(images.Multiorch, multigresv1alpha1.DefaultMultiorchImage)},
		{"multipooler", cmp.Or(images.Multipooler, multigresv1alpha1.DefaultMultipoolerImage)},
		{"multigateway", cmp.Or(images.Multigateway, multigresv1alpha1.DefaultMultigatewayImage)},
	} {
		t.Logf("runtime %s=%s", component.name, component.image)
		require.Regexp(t, `@sha256:[0-9a-f]{64}$`, string(component.image), "%s must be pinned by digest", component.name)
		expected[component.name] = string(component.image)
	}
	return expected
}

type lifecycleCheck struct {
	client    client.Client
	namespace string
	name      string
	cell      string
	topoRoot  string
	gateway   endpoint
}

func (c lifecycleCheck) requireRuntimeImages(t *testing.T, expected map[string]string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()
	var pods corev1.PodList
	require.NoError(t, c.client.List(ctx, &pods, client.InNamespace(c.namespace)), "observe runtime images in Kubernetes")
	seen := make(map[string]bool)
	for _, pod := range pods.Items {
		for _, container := range slices.Concat(pod.Spec.Containers, pod.Spec.InitContainers) {
			if image, ok := expected[container.Name]; ok {
				require.Equal(t, image, container.Image, "pod %s container %s image", pod.Name, container.Name)
				seen[container.Name] = true
			}
		}
	}
	for name := range expected {
		require.True(t, seen[name], "no pod ran runtime component %s", name)
	}
}

func (c lifecycleCheck) create(t *testing.T) (int64, map[string]endpoint) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 9*time.Minute)
	defer cancel()
	_, err := c.gateway.psql(ctx, "SET statement_timeout = '8min'", createSQL)
	require.NoError(t, err, createSQL)
	var id int64
	waitForCheck(t, "active catalog entry", func(ctx context.Context) error {
		out, err := c.gateway.psql(ctx, catalogSQL)
		if err != nil {
			return err
		}
		idText, state, _ := strings.Cut(out, "|")
		id, err = strconv.ParseInt(idText, 10, 64)
		if state != "active" || err != nil || id <= 0 {
			return fmt.Errorf("expected a positive tablegroup ID in active state, got %q", out)
		}
		return nil
	})

	var primaries map[string]endpoint
	waitForCheck(t, fmt.Sprintf("serving primaries for catalog ID %d", id), func(ctx context.Context) error {
		poolers, err := c.poolers(ctx)
		if err != nil {
			return err
		}
		primaries = make(map[string]endpoint)
		for _, pooler := range poolers {
			ref := pooler.GetShardRef()
			state := pooler.GetLifecycleStatus().GetStatus()
			if ref.GetDatabase() == database && ref.GetTableGroup() == tablegroup && pooler.GetTablegroupId() == id &&
				pooler.GetRoutingState().GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY &&
				pooler.GetServingStatus() == clustermetadatapb.PoolerServingStatus_SERVING &&
				(state == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE ||
					state == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN) &&
				pooler.GetHostname() != "" {
				primaries[ref.GetShard()] = endpoint{c.namespace, c.gateway.pod, pooler.GetHostname()}
			}
		}
		for _, shard := range ranges {
			if _, ok := primaries[shard]; !ok {
				return fmt.Errorf("shard %s has no serving primary; observed primaries %v", shard, primaries)
			}
		}
		return nil
	})
	t.Logf("active catalog ID %d serving primaries %v", id, primaries)
	return id, primaries
}

func (c lifecycleCheck) labels() client.MatchingLabels {
	return client.MatchingLabels{
		metadata.LabelMultigresCluster: c.name, metadata.LabelMultigresDatabase: database,
		metadata.LabelMultigresTableGroup: tablegroup,
	}
}

func waitForCheck(t *testing.T, name string, check func(context.Context) error) {
	t.Helper()
	var observed error
	err := wait.PollUntilContextTimeout(t.Context(), 2*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		ctx, cancel := context.WithTimeout(ctx, requestTimeout)
		defer cancel()
		observed = check(ctx)
		return observed == nil, nil
	})
	require.NoErrorf(t, err, "%s: %v", name, observed)
}

type endpoint struct{ namespace, pod, host string }

func (e endpoint) psql(ctx context.Context, statements ...string) (string, error) {
	args := []string{"exec", e.pod, "-c", "postgres", "--",
		"env", "PGPASSWORD=postgres", "PGCONNECT_TIMEOUT=5", "psql", "-XqAtw", "-v", "ON_ERROR_STOP=1",
		"-h", e.host, "-U", "postgres", "-d", database}
	for _, stmt := range statements {
		args = append(args, "-c", stmt)
	}
	return kubectl(ctx, e.namespace, args...)
}

func (e endpoint) query(t *testing.T, stmt string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()
	out, err := e.psql(ctx, stmt)
	require.NoError(t, err, "%s via %s", stmt, e.host)
	return out
}

func (e endpoint) checkDefault(t *testing.T, expected int) {
	t.Helper()
	e.query(t, "UPDATE default_probe SET checks = checks + 1")
	require.Equal(t, fmt.Sprintf("still serving|%d", expected),
		e.query(t, "SELECT marker, checks FROM default_probe"), "default tablegroup continuity")
}

func kubectl(ctx context.Context, namespace string, args ...string) (string, error) {
	args = append([]string{"--kubeconfig", cluster.Kubeconfig, "-n", namespace}, args...)
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("kubectl %s: %w: %s", args[4], err, stderr.String())
	}
	return strings.TrimSpace(string(out)), nil
}
