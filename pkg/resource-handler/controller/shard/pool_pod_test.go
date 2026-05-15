package shard

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func newTestShard() *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			UID:       "test-uid",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
		},
	}
}

func newTestPoolSpec() multigresv1alpha1.PoolSpec {
	return multigresv1alpha1.PoolSpec{
		Type: "replica",
		Storage: multigresv1alpha1.StorageSpec{
			Size: "10Gi",
		},
	}
}

func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	return scheme
}

func TestBuildPoolPod_BasicStructure(t *testing.T) {
	shard := newTestShard()
	pool := newTestPoolSpec()

	pod, err := BuildPoolPod(shard, "main", "z1", pool, 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pod.Namespace != "default" {
		t.Errorf("namespace = %q, want %q", pod.Namespace, "default")
	}

	// Verify owner reference
	if len(pod.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(pod.OwnerReferences))
	}
	if pod.OwnerReferences[0].Name != "test-shard" {
		t.Errorf("owner name = %q, want %q", pod.OwnerReferences[0].Name, "test-shard")
	}
	if pod.OwnerReferences[0].Kind != "Shard" {
		t.Errorf("owner kind = %q, want %q", pod.OwnerReferences[0].Kind, "Shard")
	}

	// Verify labels
	expectedLabels := map[string]string{
		"app.kubernetes.io/instance":   "test-cluster",
		"app.kubernetes.io/component":  PoolComponentName,
		"app.kubernetes.io/managed-by": "multigres-operator",
		"multigres.com/cluster":        "test-cluster",
		"multigres.com/cell":           "z1",
		"multigres.com/pool":           "main",
		"multigres.com/shard":          "0-inf",
		"multigres.com/database":       "postgres",
		"multigres.com/tablegroup":     "default",
	}
	for k, want := range expectedLabels {
		if got := pod.Labels[k]; got != want {
			t.Errorf("label %q = %q, want %q", k, got, want)
		}
	}

	if got := pod.Annotations[metadata.AnnotationProjectRef]; got != "test-cluster" {
		t.Errorf("annotation %q = %q, want %q", metadata.AnnotationProjectRef, got, "test-cluster")
	}
}

func TestBuildPoolPod_ProjectRefAnnotation(t *testing.T) {
	tests := map[string]struct {
		annotations map[string]string
		want        string
	}{
		"falls back to cluster name": {
			want: "test-cluster",
		},
		"uses explicit project ref": {
			annotations: map[string]string{
				metadata.AnnotationProjectRef: "proj_123",
			},
			want: "proj_123",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			shard := newTestShard()
			shard.Annotations = tc.annotations

			pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got := pod.Annotations[metadata.AnnotationProjectRef]; got != tc.want {
				t.Fatalf("annotation %q = %q, want %q", metadata.AnnotationProjectRef, got, tc.want)
			}
		})
	}
}

func TestBuildPoolPod_PrometheusScrapeAnnotations(t *testing.T) {
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantAnnotations := map[string]string{
		metadata.AnnotationPrometheusScrape: "true",
		metadata.AnnotationPrometheusPort:   "9187",
		metadata.AnnotationPrometheusPath:   "/metrics",
	}
	for key, want := range wantAnnotations {
		if got := pod.Annotations[key]; got != want {
			t.Fatalf("annotation %q = %q, want %q", key, got, want)
		}
	}
}

func TestBuildPoolPod_Containers(t *testing.T) {
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf(
			"expected 1 init container (multipooler sidecar), got %d",
			len(pod.Spec.InitContainers),
		)
	}
	if pod.Spec.InitContainers[0].Name != "multipooler" {
		t.Errorf(
			"init container name = %q, want %q",
			pod.Spec.InitContainers[0].Name,
			"multipooler",
		)
	}

	if len(pod.Spec.Containers) != 2 {
		t.Fatalf(
			"expected 2 containers (postgres + postgres-exporter), got %d",
			len(pod.Spec.Containers),
		)
	}
	if pod.Spec.Containers[0].Name != "postgres" {
		t.Errorf("container name = %q, want %q", pod.Spec.Containers[0].Name, "postgres")
	}
	if pod.Spec.Containers[1].Name != "postgres-exporter" {
		t.Errorf(
			"container name = %q, want %q",
			pod.Spec.Containers[1].Name,
			"postgres-exporter",
		)
	}
}

func TestBuildPoolPod_Volumes(t *testing.T) {
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	volumeNames := make(map[string]bool)
	for _, v := range pod.Spec.Volumes {
		volumeNames[v.Name] = true
	}

	required := []string{DataVolumeName, "backup-data", "socket-dir", PgHbaVolumeName}
	for _, name := range required {
		if !volumeNames[name] {
			t.Errorf("missing required volume %q", name)
		}
	}

	// Verify data volume references PVC
	for _, v := range pod.Spec.Volumes {
		if v.Name == DataVolumeName {
			if v.PersistentVolumeClaim == nil {
				t.Fatal("data volume should reference a PVC")
			}
			pvcName := v.PersistentVolumeClaim.ClaimName
			if pvcName == "" {
				t.Error("data volume PVC claim name is empty")
			}
		}
	}
}

func TestBuildPoolPod_SecurityContext(t *testing.T) {
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pod.Spec.SecurityContext != nil {
		t.Errorf(
			"pod security context = %v, want nil when fsGroup is not configured",
			pod.Spec.SecurityContext,
		)
	}

	if pod.Spec.TerminationGracePeriodSeconds == nil ||
		*pod.Spec.TerminationGracePeriodSeconds != 30 {
		t.Errorf(
			"terminationGracePeriodSeconds = %v, want 30",
			pod.Spec.TerminationGracePeriodSeconds,
		)
	}
}

func TestBuildPoolPod_SecurityContextWithFSGroup(t *testing.T) {
	pool := newTestPoolSpec()
	pool.FSGroup = ptr.To(int64(1234))

	pod, err := BuildPoolPod(newTestShard(), "main", "z1", pool, 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pod.Spec.SecurityContext == nil {
		t.Fatal("pod security context is nil")
	}
	if pod.Spec.SecurityContext.FSGroup == nil || *pod.Spec.SecurityContext.FSGroup != 1234 {
		t.Errorf("FSGroup = %v, want 1234", pod.Spec.SecurityContext.FSGroup)
	}
}

func TestBuildContainerSecurityContext(t *testing.T) {
	t.Run("nil fsGroup", func(t *testing.T) {
		sc := buildContainerSecurityContext(nil, defaultPostgresRunAsUserUID)
		assert.True(t, *sc.RunAsNonRoot)
		assert.Equal(t, defaultPostgresRunAsUserUID, *sc.RunAsUser)
		assert.Equal(t, defaultPostgresRunAsUserUID, *sc.RunAsGroup)
	})

	t.Run("with fsGroup", func(t *testing.T) {
		sc := buildContainerSecurityContext(ptr.To(int64(999)), defaultPostgresRunAsUserUID)
		assert.True(t, *sc.RunAsNonRoot)
		assert.Equal(t, int64(999), *sc.RunAsUser)
		assert.Equal(t, int64(999), *sc.RunAsGroup)
	})

	t.Run("alpine fsGroup", func(t *testing.T) {
		sc := buildContainerSecurityContext(ptr.To(int64(70)), defaultPostgresRunAsUserUID)
		assert.True(t, *sc.RunAsNonRoot)
		assert.Equal(t, int64(70), *sc.RunAsUser)
		assert.Equal(t, int64(70), *sc.RunAsGroup)
	})
}

func TestBuildPoolPod_SpecHash(t *testing.T) {
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hash, ok := pod.Annotations[metadata.AnnotationSpecHash]
	if !ok {
		t.Fatal("spec-hash annotation missing")
	}
	if hash == "" {
		t.Error("spec-hash annotation is empty")
	}
	if len(hash) != 8 {
		t.Errorf("spec-hash length = %d, want 8 (FNV-1a 32-bit hex)", len(hash))
	}
}

func TestBuildPoolPod_NoFinalizers(t *testing.T) {
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pod.Finalizers) != 0 {
		t.Errorf("finalizers = %v, want none", pod.Finalizers)
	}
}

func TestBuildPoolPod_Affinity(t *testing.T) {
	pool := newTestPoolSpec()
	pool.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "disk-type",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"ssd"},
						},
					},
				}},
			},
		},
	}

	pod, err := BuildPoolPod(newTestShard(), "main", "z1", pool, 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		t.Fatal("affinity not set on pod")
	}
}

func TestBuildPoolPod_Tolerations(t *testing.T) {
	pool := newTestPoolSpec()
	pool.Tolerations = []corev1.Toleration{
		{
			Key:      "dedicated",
			Operator: corev1.TolerationOpEqual,
			Value:    "database",
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	pod, err := BuildPoolPod(newTestShard(), "main", "z1", pool, 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pod.Spec.Tolerations) != 1 {
		t.Fatalf("expected 1 toleration, got %d", len(pod.Spec.Tolerations))
	}
	if pod.Spec.Tolerations[0].Key != "dedicated" {
		t.Errorf("toleration key = %q, want %q", pod.Spec.Tolerations[0].Key, "dedicated")
	}
	if pod.Spec.Tolerations[0].Value != "database" {
		t.Errorf("toleration value = %q, want %q", pod.Spec.Tolerations[0].Value, "database")
	}
}

func TestComputeSpecHash_ChangesOnTolerations(t *testing.T) {
	pool1 := newTestPoolSpec()
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", pool1, 0, testScheme())

	pool2 := newTestPoolSpec()
	pool2.Tolerations = []corev1.Toleration{
		{
			Key:      "dedicated",
			Operator: corev1.TolerationOpEqual,
			Value:    "database",
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}
	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", pool2, 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	if hash1 == hash2 {
		t.Error("spec hash should differ when tolerations change")
	}
}

func TestComputeSpecHash_ChangesOnFSGroup(t *testing.T) {
	pool1 := newTestPoolSpec()
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", pool1, 0, testScheme())

	pool2 := newTestPoolSpec()
	pool2.FSGroup = ptr.To(int64(1234))
	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", pool2, 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	if hash1 == hash2 {
		t.Error("spec hash should differ when fsGroup changes")
	}
}

func TestBuildPoolPod_NodeSelector(t *testing.T) {
	shard := newTestShard()
	shard.Spec.CellTopologyLabels = map[multigresv1alpha1.CellName]map[string]string{
		"z1": {"topology.kubernetes.io/zone": "us-east-1a"},
	}

	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pod.Spec.NodeSelector == nil {
		t.Fatal("node selector is nil")
	}
	if pod.Spec.NodeSelector["topology.kubernetes.io/zone"] != "us-east-1a" {
		t.Errorf("node selector zone = %q, want %q",
			pod.Spec.NodeSelector["topology.kubernetes.io/zone"], "us-east-1a")
	}
}

func TestBuildPoolPod_Hostname(t *testing.T) {
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pod.Spec.Hostname != pod.Name {
		t.Errorf("hostname = %q, should match pod name %q", pod.Spec.Hostname, pod.Name)
	}
	if pod.Spec.Subdomain == "" {
		t.Error("subdomain (headless service name) should not be empty")
	}
}

func TestBuildPoolPod_ServiceAccountName(t *testing.T) {
	t.Run("set when S3 serviceAccountName configured", func(t *testing.T) {
		shard := newTestShard()
		shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket:             "my-bucket",
				Region:             "us-east-1",
				ServiceAccountName: "multigres-backup",
			},
		}
		pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pod.Spec.ServiceAccountName != "multigres-backup" {
			t.Errorf(
				"ServiceAccountName = %q, want %q",
				pod.Spec.ServiceAccountName,
				"multigres-backup",
			)
		}
	})

	t.Run("empty when no backup config", func(t *testing.T) {
		pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pod.Spec.ServiceAccountName != "" {
			t.Errorf("ServiceAccountName = %q, want empty", pod.Spec.ServiceAccountName)
		}
	})

	t.Run("empty when S3 has no serviceAccountName", func(t *testing.T) {
		shard := newTestShard()
		shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "my-bucket",
				Region: "us-east-1",
			},
		}
		pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pod.Spec.ServiceAccountName != "" {
			t.Errorf("ServiceAccountName = %q, want empty", pod.Spec.ServiceAccountName)
		}
	})

	t.Run("empty when backup is filesystem type", func(t *testing.T) {
		shard := newTestShard()
		shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeFilesystem,
		}
		pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pod.Spec.ServiceAccountName != "" {
			t.Errorf("ServiceAccountName = %q, want empty", pod.Spec.ServiceAccountName)
		}
	})
}

func TestComputeSpecHash_ChangesOnServiceAccountName(t *testing.T) {
	shard := newTestShard()
	pod1, _ := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())

	shardWithSA := newTestShard()
	shardWithSA.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket:             "my-bucket",
			Region:             "us-east-1",
			ServiceAccountName: "multigres-backup",
		},
	}
	pod2, _ := BuildPoolPod(shardWithSA, "main", "z1", newTestPoolSpec(), 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	if hash1 == hash2 {
		t.Error("spec hash should differ when ServiceAccountName is added")
	}
}

func TestComputeSpecHash_Deterministic(t *testing.T) {
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	if hash1 != hash2 {
		t.Errorf("spec hash not deterministic: %q != %q", hash1, hash2)
	}
}

func TestComputeSpecHash_ChangesOnDrift(t *testing.T) {
	pool := newTestPoolSpec()
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", pool, 0, testScheme())

	// Build a second pod with different affinity (changes operator-managed fields)
	pool2 := newTestPoolSpec()
	pool2.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "disk-type",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"ssd"},
						},
					},
				}},
			},
		},
	}
	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", pool2, 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	if hash1 == hash2 {
		t.Error("spec hash should differ when affinity changes")
	}
}

func TestComputeSpecHash_ChangesOnValueFromDrift(t *testing.T) {
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	pod1.Spec.Containers[0].Env = append(pod1.Spec.Containers[0].Env, corev1.EnvVar{
		Name: "TEST_ENV",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "secret1"},
				Key:                  "key1",
			},
		},
	})

	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	pod2.Spec.Containers[0].Env = append(pod2.Spec.Containers[0].Env, corev1.EnvVar{
		Name: "TEST_ENV",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "secret2"},
				Key:                  "key1",
			},
		},
	})

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	if hash1 == hash2 {
		t.Error("spec hash should differ when ValueFrom secret name changes")
	}
}

func TestComputeSpecHash_ChangesOnEnvFromDrift(t *testing.T) {
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	pod1.Spec.Containers[0].EnvFrom = append(pod1.Spec.Containers[0].EnvFrom, corev1.EnvFromSource{
		ConfigMapRef: &corev1.ConfigMapEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "config1"},
		},
	})

	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	pod2.Spec.Containers[0].EnvFrom = append(pod2.Spec.Containers[0].EnvFrom, corev1.EnvFromSource{
		ConfigMapRef: &corev1.ConfigMapEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "config2"},
		},
	})

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	if hash1 == hash2 {
		t.Error("spec hash should differ when EnvFrom config map name changes")
	}
}

func TestBuildPoolPodName_Truncation(t *testing.T) {
	shard := newTestShard()
	shard.Labels["multigres.com/cluster"] = "very-long-cluster-name-for-testing"
	shard.Spec.DatabaseName = "long-database-name"
	shard.Spec.TableGroupName = "long-tablegroup"
	shard.Spec.ShardName = "long-shard"

	name := BuildPoolPodName(shard, "main-pool", "us-east-1a", 99)

	if len(name) > 63 {
		t.Errorf("pod name %q exceeds 63 chars (len=%d)", name, len(name))
	}
	if !strings.HasSuffix(name, "-99") {
		t.Errorf("pod name %q should end with -99", name)
	}
}

func TestBuildPoolPodName_ShortName(t *testing.T) {
	name := BuildPoolPodName(newTestShard(), "main", "z1", 0)

	if len(name) > 63 {
		t.Errorf("pod name %q exceeds 63 chars (len=%d)", name, len(name))
	}
	if !strings.HasSuffix(name, "-0") {
		t.Errorf("pod name %q should end with -0", name)
	}
	// Pod name should contain meaningful parts
	if !strings.Contains(name, "pool") {
		t.Errorf("pod name %q should contain 'pool'", name)
	}
}

func TestComputeSpecHash_ChangesOnPostgresConfigHash(t *testing.T) {
	shard1 := newTestShard()
	pod1, _ := BuildPoolPod(shard1, "main", "z1", newTestPoolSpec(), 0, testScheme())

	shard2 := newTestShard()
	shard2.Annotations = map[string]string{
		metadata.AnnotationPostgresConfigHash: "abc123",
	}
	pod2, _ := BuildPoolPod(shard2, "main", "z1", newTestPoolSpec(), 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)
	if hash1 == hash2 {
		t.Error("spec hash should differ when postgres config hash annotation is added")
	}
}

func TestComputeSpecHash_ChangesOnDifferentPostgresConfigHash(t *testing.T) {
	shard1 := newTestShard()
	shard1.Annotations = map[string]string{
		metadata.AnnotationPostgresConfigHash: "hash-v1",
	}
	pod1, _ := BuildPoolPod(shard1, "main", "z1", newTestPoolSpec(), 0, testScheme())

	shard2 := newTestShard()
	shard2.Annotations = map[string]string{
		metadata.AnnotationPostgresConfigHash: "hash-v2",
	}
	pod2, _ := BuildPoolPod(shard2, "main", "z1", newTestPoolSpec(), 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)
	if hash1 == hash2 {
		t.Error("spec hash should differ when postgres config hash value changes")
	}
}

func TestBuildPoolPod_PropagatesPostgresConfigHash(t *testing.T) {
	shard := newTestShard()
	shard.Annotations = map[string]string{
		metadata.AnnotationPostgresConfigHash: "deadbeef",
	}

	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := pod.Annotations[metadata.AnnotationPostgresConfigHash]
	if got != "deadbeef" {
		t.Errorf("postgres config hash annotation = %q, want %q", got, "deadbeef")
	}
}

func TestBuildPoolPod_OmitsPostgresConfigHashWhenAbsent(t *testing.T) {
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := pod.Annotations[metadata.AnnotationPostgresConfigHash]; ok {
		t.Error("postgres config hash annotation should not be present when shard has none")
	}
}
