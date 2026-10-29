//go:build integration
// +build integration

package shard

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/images"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// storedByPreviousOperator is a Shard exactly as the operator that predates the
// sidecar wrote it: no sidecarBackupLocation, no images.pgbackrestd and no
// shardServiceAccountName. It is created as raw JSON so the typed API cannot
// fill in a field that operator never knew about.
func storedByPreviousOperator(name string, lite bool) *unstructured.Unstructured {
	spec := map[string]any{
		"databaseName":   "testdb",
		"tableGroupName": "default",
		"shardName":      "0-inf",
		"postgresPasswordSecretRef": map[string]any{
			"name": "multigres-admin-password", "key": PostgresPasswordSecretKey,
		},
		"images": map[string]any{
			"multiorch":   "ghcr.io/multigres/multigres:old",
			"multipooler": "ghcr.io/multigres/multigres:old",
			"postgres":    "postgres:17",
		},
		"globalTopoServer": map[string]any{
			"address": "global-topo:2379", "rootPath": "/multigres/global", "implementation": "etcd",
		},
		"multiorch": map[string]any{"cells": []any{"zone1"}},
		"pools": map[string]any{
			"default": map[string]any{
				"cells":           []any{"zone1"},
				"type":            "readWrite",
				"replicasPerCell": int64(1),
				"storage":         map[string]any{"size": "1Gi"},
			},
		},
		"backup": map[string]any{
			"type": "filesystem",
			"filesystem": map[string]any{
				"path":    "/backups",
				"storage": map[string]any{"size": "1Gi", "class": "fast"},
			},
		},
	}
	if lite {
		spec["mode"] = multigresv1alpha1.ModeLite
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "multigres.com/v1alpha1",
		"kind":       "Shard",
		"metadata": map[string]any{
			"name": name, "namespace": "default",
			"labels": map[string]any{metadata.LabelMultigresCluster: "upgrade"},
		},
		"spec": spec,
	}}
}

// The fake client keeps whatever Go struct it is handed, so it cannot show how
// a Shard stored before the new fields existed behaves under the new operator.
// This stores one through a real API server, then drives the upgrade: the new
// operator must leave the existing pod and claim alone until the cluster
// controller rewrites the Shard, and after that roll the pod exactly once onto
// the sidecar template without wedging the shared backup claim.
func TestUpgradeInPlace_ShardStoredByPreviousOperator(t *testing.T) {
	cfg := testutil.SetUpEnvtest(
		t,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	scheme := testScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	c := testutil.SetUpClient(t, cfg, scheme)
	ctx := context.Background()
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "multigres-admin-password", Namespace: "default"},
		Data:       map[string][]byte{PostgresPasswordSecretKey: []byte("pw")},
	}))

	for _, lite := range []bool{false, true} {
		name := "upgrade-pool"
		if lite {
			name = "upgrade-lite"
		}
		t.Run(name, func(t *testing.T) {
			require.NoError(t, c.Create(ctx, storedByPreviousOperator(name, lite)))
			stored := &multigresv1alpha1.Shard{}
			require.NoError(t, c.Get(ctx,
				types.NamespacedName{Namespace: "default", Name: name}, stored))
			require.Empty(t, stored.Spec.Images.PgBackRestd, "the old operator never set it")
			require.Nil(t, stored.Spec.SidecarBackupLocation)
			require.Empty(t, stored.Spec.ShardServiceAccountName)

			r := &ShardReconciler{
				Client: c, APIReader: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100),
			}
			poolSpec := stored.Spec.Pools["default"]

			// The pod and the claim the previous operator left behind. The
			// claim is applied by the same field manager, from the shard-level
			// backup config that operator derived it from.
			oldBackup := stored.Spec.Backup.DeepCopy()
			claimOwner := stored.DeepCopy()
			claimOwner.Spec.Images.PgBackRestd = "old"
			claimOwner.Spec.SidecarBackupLocation = oldBackup
			require.NoError(t, r.reconcileSharedBackupPVC(ctx, claimOwner))
			claim := getSharedBackupPVC(t, c, stored)
			require.NotNil(t, claim.Spec.StorageClassName)
			require.Equal(t, "fast", *claim.Spec.StorageClassName)

			legacy := legacyPodOf(t, stored, claimOwner, poolSpec, lite, scheme)

			// Stale shard: nothing is rebuilt, nothing is rolled, nothing wedges.
			_, err := r.Reconcile(ctx, requestOf(stored))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "predates the pgBackRest sidecar")
			assert.False(t, podNeedsUpdateFor(legacy, stored, poolSpec, lite, scheme),
				"a stale shard must not roll the existing pod")

			// The cluster controller's rewrite, from the production builders.
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(stored), stored))
			upgraded := rewrittenByClusterController(t, stored, oldBackup)
			require.NoError(t, c.Update(ctx, upgraded))

			require.NoError(t, r.reconcileSharedBackupPVC(ctx, upgraded))
			require.NoError(t, r.reconcileSharedBackupPVC(ctx, upgraded))
			after := getSharedBackupPVC(t, c, upgraded)
			require.NotNil(t, after.Spec.StorageClassName)
			assert.Equal(t, "fast", *after.Spec.StorageClassName, "the claim keeps its class")
			assert.Equal(t, claim.Spec.VolumeName, after.Spec.VolumeName)

			assert.True(t, podNeedsUpdateFor(legacy, upgraded, poolSpec, lite, scheme),
				"the pod without the sidecar must roll to the new template")
			desired := desiredPodOf(t, upgraded, poolSpec, lite, scheme)
			assert.True(t, hasInitContainer(desired, PgBackRestSidecarContainerName))
			assert.False(t, podNeedsUpdateFor(desired, upgraded, poolSpec, lite, scheme),
				"once rolled, the pod is current: it rolls exactly once")
			assert.Equal(t, claim.Name, volumeClaim(desired, BackupVolumeName),
				"the rolled pod mounts the same backup claim")
		})
	}
}

func hasInitContainer(pod *corev1.Pod, name string) bool {
	for _, ic := range pod.Spec.InitContainers {
		if ic.Name == name {
			return true
		}
	}
	return false
}

func requestOf(s *multigresv1alpha1.Shard) ctrl.Request {
	return ctrlRequest(s)
}

func getSharedBackupPVC(
	t *testing.T,
	c client.Client,
	s *multigresv1alpha1.Shard,
) *corev1.PersistentVolumeClaim {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{
		Namespace: "default", Name: BuildSharedBackupPVCName(s),
	}, pvc))
	return pvc
}

func volumeClaim(pod *corev1.Pod, volume string) string {
	for _, v := range pod.Spec.Volumes {
		if v.Name == volume && v.PersistentVolumeClaim != nil {
			return v.PersistentVolumeClaim.ClaimName
		}
	}
	return ""
}

func desiredPodOf(
	t *testing.T,
	s *multigresv1alpha1.Shard,
	pool multigresv1alpha1.PoolSpec,
	lite bool,
	scheme *runtime.Scheme,
) *corev1.Pod {
	t.Helper()
	var (
		pod *corev1.Pod
		err error
	)
	if lite {
		pod, err = BuildLitePod(s, "default", "zone1", pool, scheme)
	} else {
		pod, err = BuildPoolPod(s, "default", "zone1", pool, 0, scheme)
	}
	require.NoError(t, err)
	return pod
}

// podNeedsUpdateFor asks the controller's own staleness question: the pool
// rollout's podNeedsUpdate for a pool pod, the spec-hash comparison the Lite
// reconcile makes for a Lite pod.
func podNeedsUpdateFor(
	existing *corev1.Pod,
	s *multigresv1alpha1.Shard,
	pool multigresv1alpha1.PoolSpec,
	lite bool,
	scheme *runtime.Scheme,
) bool {
	if lite {
		desired, err := BuildLitePod(s, "default", "zone1", pool, scheme)
		if err != nil {
			return false
		}
		return existing.Annotations[metadata.AnnotationSpecHash] !=
			desired.Annotations[metadata.AnnotationSpecHash]
	}
	return podNeedsUpdate(existing, s, "default", "zone1", pool, 0, scheme)
}

// legacyPodOf is the pod the previous operator would have left running: the
// pod the current builder makes with everything the sidecar added taken out,
// and its spec hash recomputed over what remains.
func legacyPodOf(
	t *testing.T,
	_ *multigresv1alpha1.Shard,
	complete *multigresv1alpha1.Shard,
	pool multigresv1alpha1.PoolSpec,
	lite bool,
	scheme *runtime.Scheme,
) *corev1.Pod {
	t.Helper()
	pod := desiredPodOf(t, complete, pool, lite, scheme)
	sidecarVolumes := map[string]bool{
		PgBackRestWALVolumeName: true, PgBackRestControlVolumeName: true,
		PgBackRestStateVolumeName: true, PgBackRestCertVolumeName: true,
		PgBackRestCipherKeyVolumeName: true, PgBackRestS3VolumeName: true,
	}
	pod.Spec.InitContainers = slices.DeleteFunc(pod.Spec.InitContainers,
		func(c corev1.Container) bool { return c.Name == PgBackRestSidecarContainerName })
	pod.Spec.Volumes = slices.DeleteFunc(pod.Spec.Volumes,
		func(v corev1.Volume) bool { return sidecarVolumes[v.Name] })
	pod.Spec.AutomountServiceAccountToken = nil
	pod.Spec.ServiceAccountName = ""
	delete(pod.Annotations, SkipContainersAnnotation)
	pod.Annotations[metadata.AnnotationSpecHash] = ComputeSpecHash(pod)
	require.False(t, hasInitContainer(pod, PgBackRestSidecarContainerName))
	return pod
}

// rewrittenByClusterController is the stored Shard after the cluster controller
// has reconciled it under the new operator: the three sidecar fields come from
// the production cluster and tablegroup builders, the rest is what was stored.
func rewrittenByClusterController(
	t *testing.T,
	stored *multigresv1alpha1.Shard,
	backup *multigresv1alpha1.BackupConfig,
) *multigresv1alpha1.Shard {
	t.Helper()
	scheme := testScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "upgrade", Namespace: "default", UID: "c-uid"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Backup: backup,
			Images: multigresv1alpha1.ClusterImages{Multipooler: stored.Spec.Images.Multipooler},
			ShardServiceAccount: &multigresv1alpha1.ShardServiceAccountSpec{
				Name: "shards",
			},
		},
	}
	images.DerivePgBackRestd(&cluster.Spec.Images)
	shards := []multigresv1alpha1.ShardResolvedSpec{{Name: "0-inf"}}
	tg, err := multigrescluster.BuildTableGroup(
		cluster,
		multigresv1alpha1.DatabaseConfig{Name: "testdb"},
		&multigresv1alpha1.TableGroupConfig{Name: "default", Default: true},
		shards,
		multigresv1alpha1.GlobalTopoServerRef{},
		scheme,
	)
	require.NoError(t, err)
	built, err := tablegroup.BuildShard(tg, &shards[0], scheme)
	require.NoError(t, err)

	out := stored.DeepCopy()
	out.Spec.Images.PgBackRestd = built.Spec.Images.PgBackRestd
	out.Spec.SidecarBackupLocation = built.Spec.SidecarBackupLocation
	out.Spec.ShardServiceAccountName = built.Spec.ShardServiceAccountName
	require.Equal(t, stored.Spec.Images.Multipooler, out.Spec.Images.PgBackRestd,
		"the sidecar follows the shard's multipooler")
	require.NotNil(t, out.Spec.SidecarBackupLocation)
	return out
}
