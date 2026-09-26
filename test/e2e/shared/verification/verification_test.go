//go:build e2e

package verification_test

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
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

	"github.com/multigres/testkit/assert"
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
	ck := assert.NewCollecting(t)
	ns := cluster.CreateNamespace(t)
	createStaticRWXVolume(t, ns)
	c, err := cluster.CRClient()
	ck.Require().NoError(err, "create CR client")

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

	ck.Require().NoError(c.Create(context.Background(), cr), "create MultigresCluster")
	cluster.WaitForAllPodsReady(t, ns)

	claims := &corev1.PersistentVolumeClaimList{}
	ck.Require().NoError(c.List(context.Background(), claims,
		client.InNamespace(ns),
		client.MatchingLabels{
			metadata.LabelMultigresCluster:    cr.Name,
			metadata.LabelMultigresDatabase:   string(cr.Spec.Databases[0].Name),
			metadata.LabelMultigresTableGroup: string(cr.Spec.Databases[0].TableGroups[0].Name),
			metadata.LabelMultigresShard: string(
				cr.Spec.Databases[0].TableGroups[0].Shards[0].Name,
			),
		},
	), "list backup PVCs")
	var backupClaims []corev1.PersistentVolumeClaim
	for _, candidate := range claims.Items {
		if candidate.Labels[metadata.LabelMultigresPool] == "" {
			backupClaims = append(backupClaims, candidate)
		}
	}
	ck.Require().Len(backupClaims, 1, "backup PVC count = %d, want 1", len(backupClaims))
	claim := &backupClaims[0]
	if len(claim.Spec.AccessModes) != 1 || claim.Spec.AccessModes[0] != corev1.ReadWriteMany {
		t.Fatalf("backup PVC access modes = %v, want [ReadWriteMany]", claim.Spec.AccessModes)
	}

	pods := &corev1.PodList{}
	ck.Require().NoError(c.List(context.Background(), pods,
		client.InNamespace(ns),
		client.MatchingLabels{
			metadata.LabelMultigresCluster: cr.Name,
			metadata.LabelMultigresPool:    "default",
		},
	), "list pooler pods")
	ck.Require().Len(pods.Items, 4, "pooler pod count = %d, want 4", len(pods.Items))
	for _, pod := range pods.Items {
		var mountedClaim string
		for _, volume := range pod.Spec.Volumes {
			if volume.Name == shardcontroller.BackupVolumeName &&
				volume.PersistentVolumeClaim != nil {
				mountedClaim = volume.PersistentVolumeClaim.ClaimName
				break
			}
		}
		ck.Eq(
			claim.Name,
			mountedClaim,
			"pod %q mounts backup claim %q, want",
			pod.Name,
			mountedClaim,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err = wait.PollUntilContextCancel(
		ctx,
		3*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
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
		},
	)
	ck.Require().NoError(err, "timed out waiting for bootstrap to elect a primary")
}

// createStaticRWXVolume supplies the claim used by this test. A Kind cluster
// has one node, so a hostPath volume is enough to verify that two poolers can
// mount the same RWX claim without adding a provisioner to e2e infrastructure.
func createStaticRWXVolume(t *testing.T, namespace string) {
	t.Helper()

	name := fmt.Sprintf("e2e-rwx-%s", namespace)
	hostPath := "/var/local/multigres-e2e-rwx/" + namespace
	prepareStaticRWXHostPath(t, hostPath)

	_, err := cluster.Clientset.CoreV1().
		PersistentVolumes().
		Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: apiresource.MustParse("1Gi"),
				},
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteMany,
				},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				StorageClassName:              "e2e-rwx",
				NodeAffinity: &corev1.VolumeNodeAffinity{
					Required: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: []corev1.NodeSelectorRequirement{{
								Key:      "node-role.kubernetes.io/control-plane",
								Operator: corev1.NodeSelectorOpExists,
							}},
						}},
					},
				},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: hostPath,
						Type: ptr.To(corev1.HostPathDirectoryOrCreate),
					},
				},
			},
		}, metav1.CreateOptions{})
	assert.NewAborting(t).NoError(err, "create static RWX volume")
	t.Cleanup(func() {
		_ = cluster.Clientset.CoreV1().
			PersistentVolumes().
			Delete(context.Background(), name, metav1.DeleteOptions{})
	})
}

func prepareStaticRWXHostPath(t *testing.T, path string) {
	t.Helper()

	node := cluster.Name + "-control-plane"
	for _, args := range [][]string{{"mkdir", "-p", path}, {"chmod", "0777", path}} {
		output, err := exec.CommandContext(context.Background(), "docker", append([]string{"exec", node}, args...)...).
			CombinedOutput()
		assert.NewAborting(t).NoError(err, "%s static RWX hostPath: %v\n%s", args[0], err, output)
	}
}

func testPDB(t *testing.T) {
	t.Parallel()
	ck := assert.NewAborting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.NoError(err, "create CR client")

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
	ck.NoError(c.Create(context.Background(), cr), "create MultigresCluster")
	poolLabels := client.MatchingLabels{
		metadata.LabelAppComponent: shardcontroller.PoolComponentName,
	}
	framework.WaitForPodCount(t, c, ns, poolLabels, 4, "poolers across both pools")
	cluster.WaitForAllPodsReady(t, ns)
	shards := &multigresv1alpha1.ShardList{}
	ck.NoError(c.List(context.Background(), shards, client.InNamespace(ns)))
	ck.Len(shards.Items, 1)
	poolers := &corev1.PodList{}
	ck.NoError(c.List(context.Background(), poolers, client.InNamespace(ns), poolLabels))
	ck.Len(poolers.Items, 4)

	pdbs := framework.ListPDBs(t, c, ns)
	var shardPDBs []policyv1.PodDisruptionBudget
	for _, pdb := range pdbs {
		if owner := metav1.GetControllerOf(&pdb); owner != nil && owner.UID == shards.Items[0].UID {
			shardPDBs = append(shardPDBs, pdb)
		}
	}
	ck.Len(shardPDBs, 1)
	pdb := &shardPDBs[0]
	minimum := intstr.FromInt32(3)
	ck.EqDeep(&minimum, pdb.Spec.MinAvailable)
	ck.Nil(pdb.Spec.MaxUnavailable)
	ck.NotNil(pdb.Spec.Selector)
	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	ck.NoError(err)
	for _, pod := range poolers.Items {
		ck.True(selector.Matches(labels.Set(pod.Labels)), "shard PDB must cover %s", pod.Name)
		matches := 0
		for _, candidate := range pdbs {
			selector, err := metav1.LabelSelectorAsSelector(candidate.Spec.Selector)
			ck.NoError(err)
			if selector.Matches(labels.Set(pod.Labels)) {
				matches++
			}
		}
		ck.EqDeep(1, matches, "pooler %s must not match overlapping PDBs", pod.Name)
	}
	// Verify the Kubernetes disruption controller agrees with the desired budget.
	ck.EventuallyTrue(time.Minute, time.Second, func() bool {
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pdb), pdb); err != nil {
			return false
		}
		return pdb.Status.ObservedGeneration == pdb.Generation &&
			pdb.Status.CurrentHealthy == 4 && pdb.Status.DesiredHealthy == 3 &&
			pdb.Status.DisruptionsAllowed == 1
	})
}

func testMultiadminWeb(t *testing.T) {
	t.Parallel()
	ck := assert.NewCollecting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.Require().NoError(err, "create CR client")

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	ck.Require().NoError(c.Create(context.Background(), cr), "create MultigresCluster")
	cluster.WaitForAllPodsReady(t, ns)

	// Verify multiadminweb deployment exists (container name has a hyphen).
	dep := framework.WaitForDeployment(t, c, ns, "multiadmin-web")
	ck.GreaterOrEqual(1, dep.Status.ReadyReplicas, "multiadmin-web has")

	// Verify multiadminweb service exists.
	framework.WaitForService(t, c, ns, "http", 18100)
}

func testLogLevels(t *testing.T) {
	t.Parallel()
	ck := assert.NewAborting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.NoError(err, "create CR client")

	cr := framework.MustLoadCluster("test/e2e/fixtures/log-levels.yaml", ns)
	ck.NoError(c.Create(context.Background(), cr), "create MultigresCluster")
	cluster.WaitForAllPodsReady(t, ns)

	// Check that pods have the expected --log-level settings.
	ctx := context.Background()
	pods := &corev1.PodList{}
	ck.NoError(c.List(ctx, pods, client.InNamespace(ns)), "list pods")

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
