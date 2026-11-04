//go:build e2e

package verification_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/test/e2e/framework"
)

// TestResourceVerification verifies that the operator creates the expected
// Kubernetes resources (PDBs, deployments, services) with correct configuration.
func TestResourceVerification(t *testing.T) {
	t.Run("PDB", testPDB)
	t.Run("MultiadminWeb", testMultiadminWeb)
	t.Run("LogLevels", testLogLevels)
	t.Run("MultiCellFilesystemBackup", testMultiCellFilesystemBackup)
}

func testMultiCellFilesystemBackup(t *testing.T) {
	ns := cluster.CreateNamespace(t)
	cluster.CreateStaticRWXVolume(t, ns)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	cr.Name = "multi-cell-fs-backup"
	cr.Spec.Cells = append(cr.Spec.Cells, multigresv1alpha1.CellConfig{
		Name:   "zone-b",
		ZoneID: "us-central1-a",
	})
	cr.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
			Storage: multigresv1alpha1.StorageSpec{
				Size:        "1Gi",
				Class:       "e2e-rwx",
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			},
		},
	}
	pool := cr.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"]
	pool.Cells = []multigresv1alpha1.CellName{"zone-a", "zone-b"}
	// AT_LEAST_2 bootstrap requires a cohort that can survive one member loss.
	// With two cells, two replicas per cell provide that failure-safety margin.
	replicas := int32(2)
	pool.ReplicasPerCell = &replicas
	cr.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"] = pool

	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	cluster.WaitForAllPodsReady(t, ns)

	claims := &corev1.PersistentVolumeClaimList{}
	if err := c.List(context.Background(), claims,
		client.InNamespace(ns),
		client.MatchingLabels{
			metadata.LabelMultigresCluster:    cr.Name,
			metadata.LabelMultigresDatabase:   string(cr.Spec.Databases[0].Name),
			metadata.LabelMultigresTableGroup: string(cr.Spec.Databases[0].TableGroups[0].Name),
			metadata.LabelMultigresShard:      string(cr.Spec.Databases[0].TableGroups[0].Shards[0].Name),
		},
	); err != nil {
		t.Fatalf("list backup PVCs: %v", err)
	}
	var backupClaims []corev1.PersistentVolumeClaim
	for _, candidate := range claims.Items {
		if candidate.Labels[metadata.LabelMultigresPool] == "" {
			backupClaims = append(backupClaims, candidate)
		}
	}
	if len(backupClaims) != 1 {
		t.Fatalf("backup PVC count = %d, want 1", len(backupClaims))
	}
	claim := &backupClaims[0]
	if len(claim.Spec.AccessModes) != 1 || claim.Spec.AccessModes[0] != corev1.ReadWriteMany {
		t.Fatalf("backup PVC access modes = %v, want [ReadWriteMany]", claim.Spec.AccessModes)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods,
		client.InNamespace(ns),
		client.MatchingLabels{
			metadata.LabelMultigresCluster: cr.Name,
			metadata.LabelMultigresPool:    "default",
		},
	); err != nil {
		t.Fatalf("list pooler pods: %v", err)
	}
	if len(pods.Items) != 4 {
		t.Fatalf("pooler pod count = %d, want 4", len(pods.Items))
	}
	for _, pod := range pods.Items {
		var mountedClaim string
		for _, volume := range pod.Spec.Volumes {
			if volume.Name == shardcontroller.BackupVolumeName && volume.PersistentVolumeClaim != nil {
				mountedClaim = volume.PersistentVolumeClaim.ClaimName
				break
			}
		}
		if mountedClaim != claim.Name {
			t.Errorf("pod %q mounts backup claim %q, want %q", pod.Name, mountedClaim, claim.Name)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err = wait.PollUntilContextCancel(ctx, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		shards := &multigresv1alpha1.ShardList{}
		if err := c.List(ctx, shards,
			client.InNamespace(ns),
			client.MatchingLabels{metadata.LabelMultigresCluster: cr.Name},
		); err != nil || len(shards.Items) != 1 {
			return false, nil
		}
		for _, role := range shards.Items[0].Status.PodRoles {
			if role == "PRIMARY" {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for bootstrap to elect a primary: %v", err)
	}
}

func testPDB(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	// Four members across two pools and cells must share one shard-wide budget.
	cr.Spec.Cells = append(cr.Spec.Cells, multigresv1alpha1.CellConfig{
		Name: "zone-b", ZoneID: "us-central1-a",
	})
	pools := cr.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools
	basePool := pools["default"]
	extra := basePool.DeepCopy()
	extra.ReplicasPerCell = ptr.To(int32(1))
	extra.Cells = []multigresv1alpha1.CellName{"zone-b"}
	pools["extra"] = *extra
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	poolLabels := client.MatchingLabels{
		metadata.LabelAppComponent: shardcontroller.PoolComponentName,
	}
	framework.WaitForPodCount(t, c, ns, poolLabels, 4, "poolers across both pools")
	cluster.WaitForAllPodsReady(t, ns)
	shards := &multigresv1alpha1.ShardList{}
	require.NoError(t, c.List(context.Background(), shards, client.InNamespace(ns)))
	require.Len(t, shards.Items, 1)
	poolers := &corev1.PodList{}
	require.NoError(t, c.List(context.Background(), poolers, client.InNamespace(ns), poolLabels))
	require.Len(t, poolers.Items, 4)

	pdbs := framework.ListPDBs(t, c, ns)
	var shardPDBs []policyv1.PodDisruptionBudget
	for _, pdb := range pdbs {
		if owner := metav1.GetControllerOf(&pdb); owner != nil && owner.UID == shards.Items[0].UID {
			shardPDBs = append(shardPDBs, pdb)
		}
	}
	require.Len(t, shardPDBs, 1)
	pdb := &shardPDBs[0]
	minimum := intstr.FromInt32(3)
	require.Equal(t, &minimum, pdb.Spec.MinAvailable)
	require.Nil(t, pdb.Spec.MaxUnavailable)
	require.NotNil(t, pdb.Spec.Selector)
	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	require.NoError(t, err)
	for _, pod := range poolers.Items {
		require.True(
			t, selector.Matches(labels.Set(pod.Labels)), "shard PDB must cover %s", pod.Name,
		)
		matches := 0
		for _, candidate := range pdbs {
			selector, err := metav1.LabelSelectorAsSelector(candidate.Spec.Selector)
			require.NoError(t, err)
			if selector.Matches(labels.Set(pod.Labels)) {
				matches++
			}
		}
		require.Equal(t, 1, matches, "pooler %s must not match overlapping PDBs", pod.Name)
	}
	// Verify the Kubernetes disruption controller agrees with the desired budget.
	require.Eventually(t, func() bool {
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pdb), pdb); err != nil {
			return false
		}
		return pdb.Status.ObservedGeneration == pdb.Generation &&
			pdb.Status.CurrentHealthy == 4 && pdb.Status.DesiredHealthy == 3 &&
			pdb.Status.DisruptionsAllowed == 1
	}, time.Minute, time.Second)
}

func testMultiadminWeb(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	cluster.WaitForAllPodsReady(t, ns)

	// Verify multiadminweb deployment exists (container name has a hyphen).
	dep := framework.WaitForDeployment(t, c, ns, "multiadmin-web")
	if dep.Status.ReadyReplicas < 1 {
		t.Errorf("multiadmin-web has %d ready replicas, want >= 1", dep.Status.ReadyReplicas)
	}

	// Verify multiadminweb service exists.
	framework.WaitForService(t, c, ns, "http", 18100)
}

func testLogLevels(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	cr := framework.MustLoadCluster("test/e2e/fixtures/log-levels.yaml", ns)
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	cluster.WaitForAllPodsReady(t, ns)

	// Check that pods have the expected --log-level settings.
	ctx := context.Background()
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(ns)); err != nil {
		t.Fatalf("list pods: %v", err)
	}

	expectedLevels := map[string]string{
		"multipooler":  "warn",
		"multiorch":    "debug",
		"multiadmin":   "warn",
		"multigateway": "debug",
	}

	checkArgs := func(containerName string, args []string, podName string) {
		expectedLevel, ok := expectedLevels[containerName]
		if !ok {
			return
		}
		// Check both formats: "--log-level=value" (single arg) and
		// "--log-level" "value" (two separate args).
		for i, arg := range args {
			if arg == "--log-level="+expectedLevel {
				return
			}
			if arg == "--log-level" && i+1 < len(args) && args[i+1] == expectedLevel {
				return
			}
		}
		t.Errorf("container %s in pod %s: expected --log-level %s in args %v",
			containerName, podName, expectedLevel, args)
	}

	for _, pod := range pods.Items {
		for _, cont := range pod.Spec.Containers {
			checkArgs(cont.Name, cont.Args, pod.Name)
		}
		for _, cont := range pod.Spec.InitContainers {
			checkArgs(cont.Name, cont.Args, pod.Name)
		}
	}

	// Suppress unused variable warning.
	_ = multigresv1alpha1.MultigresCluster{}
}
