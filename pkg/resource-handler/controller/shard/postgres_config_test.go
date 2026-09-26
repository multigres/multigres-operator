package shard

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/postgresconfig"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func TestSetPostgresConfigStatus(t *testing.T) {
	r := &ShardReconciler{}

	t.Run("settled clears InProgress and stamps time", func(t *testing.T) {
		c := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{}
		r.setPostgresConfigStatus(shard, false, nil)
		st := shard.Status.PostgresConfig
		c.Require().False(st == nil || st.InProgress, "status = %+v, want InProgress false", st)
		c.NotNil(st.LastAppliedAt, "LastAppliedAt should be set when settled")
		c.Eq("", st.Error, "Error")
	})

	t.Run("in-progress sets InProgress and does not stamp", func(t *testing.T) {
		c := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{}
		r.setPostgresConfigStatus(shard, true, nil)
		st := shard.Status.PostgresConfig
		c.True(st.InProgress, "InProgress should be true during a rollout")
		c.Nil(
			st.LastAppliedAt,
			"LastAppliedAt should not be stamped while a rollout is in progress",
		)
	})

	// The key fix: a rollout driven by a PostgresConfigRef edit (or a new
	// operator baseline) never bumps the shard generation, yet InProgress must
	// still report it — the signal is content-based, not generation-based.
	t.Run("reports a rollout that does not bump generation", func(t *testing.T) {
		past := metav1.NewTime(time.Now().Add(-time.Hour))
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Generation: 5},
			Status: multigresv1alpha1.ShardStatus{
				PostgresConfig: &multigresv1alpha1.PostgresConfigStatus{LastAppliedAt: &past},
			},
		}
		r.setPostgresConfigStatus(shard, true, nil)
		assert.NewCollecting(t).
			True(shard.Status.PostgresConfig.InProgress, "InProgress should be true for a content-driven rollout at a steady generation")
	})

	t.Run("settling after a rollout re-stamps LastAppliedAt", func(t *testing.T) {
		past := metav1.NewTime(time.Now().Add(-time.Hour))
		shard := &multigresv1alpha1.Shard{
			Status: multigresv1alpha1.ShardStatus{
				PostgresConfig: &multigresv1alpha1.PostgresConfigStatus{
					InProgress:    true,
					LastAppliedAt: &past,
				},
			},
		}
		r.setPostgresConfigStatus(shard, false, nil)
		st := shard.Status.PostgresConfig
		assert.NewCollecting(t).
			False(st.InProgress, "InProgress should clear once the config settles")
		if st.LastAppliedAt == nil || !st.LastAppliedAt.After(past.Time) {
			t.Errorf("LastAppliedAt should advance on settle, got %v", st.LastAppliedAt)
		}
	})

	t.Run("steady-state settled does not churn LastAppliedAt", func(t *testing.T) {
		past := metav1.NewTime(time.Now().Add(-time.Hour))
		shard := &multigresv1alpha1.Shard{
			Status: multigresv1alpha1.ShardStatus{
				PostgresConfig: &multigresv1alpha1.PostgresConfigStatus{LastAppliedAt: &past},
			},
		}
		r.setPostgresConfigStatus(shard, false, nil)
		if got := shard.Status.PostgresConfig.LastAppliedAt; got == nil || !got.Equal(&past) {
			t.Errorf("LastAppliedAt should stay stable when already settled, got %v", got)
		}
	})

	t.Run("config error is reported and is not in progress", func(t *testing.T) {
		c := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{}
		r.setPostgresConfigStatus(shard, false, errTest)
		st := shard.Status.PostgresConfig
		c.False(st.InProgress, "InProgress should be false when a config error is reported")
		c.StrContains(st.Error, "boom", "Error")
	})
}

func TestRenderEffectiveConfig(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	t.Run("no ref returns a stable hash", func(t *testing.T) {
		c := assert.NewCollecting(t)
		r := &ShardReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
		shard := &multigresv1alpha1.Shard{ObjectMeta: metav1.ObjectMeta{Name: "s1"}}
		rc := r.renderEffectiveConfig(context.Background(), shard)
		c.Require().NoError(rc.err, "unexpected error")
		c.Len(rc.restartHash, 64, "hash length = %d, want 64", len(rc.restartHash))
	})

	t.Run("missing ref ConfigMap surfaces an error", func(t *testing.T) {
		r := &ShardReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{Name: "missing", Key: "k"},
			},
		}
		assert.NewCollecting(t).
			Error(r.renderEffectiveConfig(context.Background(), shard).err, "expected error for missing ConfigMap")
	})
}

// TestRenderEffectiveConfig_ReloadMarker proves the operator injects the
// config-version marker (see postgresconfig.ReloadMarkerGUC) into both the
// delivered file and the reload-safe expected settings, and that its value is the
// reload-hash — the wiring that makes a removal-only reload-safe change verifiable
// (see postgresconfig.TestReloadMarkerDetectsRemoval for the removal semantics).
func TestRenderEffectiveConfig_ReloadMarker(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	render := func(cfg map[string]string) renderedConfig {
		r := &ShardReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "s1"},
			Spec:       multigresv1alpha1.ShardSpec{PostgresConfig: cfg},
		}
		rc := r.renderEffectiveConfig(context.Background(), shard)
		c.Require().NoError(rc.err, "renderEffectiveConfig error")
		return rc
	}

	// A reload-safe param (log_min_duration_statement, superuser context) that the
	// operator baseline does not set, so removing it truly drops it from the render.
	rc := render(map[string]string{"log_min_duration_statement": "500ms"})

	// The marker lands in the delivered file and in the RPC expected settings,
	// with its value equal to the reload-hash.
	c.StrContains(
		rc.content,
		postgresconfig.ReloadMarkerGUC+" = '"+rc.reloadHash+"'",
		"delivered config missing marker line for %q:\n",
		postgresconfig.ReloadMarkerGUC,
	)
	got := rc.reloadSettings[postgresconfig.ReloadMarkerGUC]
	c.Eq(
		rc.reloadHash,
		got,
		"reloadSettings[%q] = %q, want reload-hash",
		postgresconfig.ReloadMarkerGUC,
		got,
	)

	// Removing the reload-safe param moves the reload-hash (hence the marker) but
	// leaves the restart-hash untouched — still a reload, not a pod recreation.
	rcRemoved := render(nil)
	c.Eq(rcRemoved.restartHash, rc.restartHash, "restart-hash moved on a reload-only removal")
	c.Require().
		NotEq(rcRemoved.reloadHash, rc.reloadHash, "reload-hash did not move when the reload-safe param was removed (still")
	c.NotEq(
		rcRemoved.reloadSettings[postgresconfig.ReloadMarkerGUC],
		rc.reloadSettings[postgresconfig.ReloadMarkerGUC],
		"marker did not move on a reload-safe removal; a stale mount would pass the gate",
	)
}

var errTest = errTestType("boom")

type errTestType string

func (e errTestType) Error() string { return string(e) }

func TestShardClusterName(t *testing.T) {
	t.Run("joins cluster/db/tablegroup/shard", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "obj-name",
				Labels: map[string]string{metadata.LabelMultigresCluster: "mycluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "mydb",
				TableGroupName: "mytg",
				ShardName:      "0",
			},
		}
		assert.NewCollecting(t).
			Eq("mycluster/mydb/mytg/0", shardClusterName(shard), "shardClusterName")
	})

	t.Run("drops empty components", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{metadata.LabelMultigresCluster: "c"},
			},
			Spec: multigresv1alpha1.ShardSpec{ShardName: "0"},
		}
		assert.NewCollecting(t).Eq("c/0", shardClusterName(shard), "shardClusterName")
	})

	t.Run("falls back to object name without cluster label", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{ObjectMeta: metav1.ObjectMeta{Name: "fallback-shard"}}
		assert.NewCollecting(t).Eq("fallback-shard", shardClusterName(shard), "shardClusterName")
	})

	t.Run("rendered config carries the shard cluster_name", func(t *testing.T) {
		c := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "obj-name",
				Labels: map[string]string{metadata.LabelMultigresCluster: "mycluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "mydb",
				TableGroupName: "mytg",
				ShardName:      "0",
			},
		}
		rendered, _, err := renderPostgresConfig(shard, "")
		c.Require().NoError(err, "unexpected error")
		c.StrContains(
			rendered,
			"cluster_name = 'mycluster/mydb/mytg/0'",
			"rendered config missing shard cluster_name:\n",
		)
	})
}

func TestReduceShardResources(t *testing.T) {
	t.Run(
		"max mem/cpu, min disk across pools with limit-then-request fallback",
		func(t *testing.T) {
			c := assert.NewCollecting(t)
			shard := &multigresv1alpha1.Shard{
				Spec: multigresv1alpha1.ShardSpec{
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"a": {
							// Limits present → used over requests.
							Postgres: multigresv1alpha1.ContainerConfig{
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										corev1.ResourceMemory: resource.MustParse("1Gi"),
										corev1.ResourceCPU:    resource.MustParse("2"),
									},
								},
							},
							Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
						},
						"b": {
							// No limits → falls back to requests.
							Postgres: multigresv1alpha1.ContainerConfig{
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceMemory: resource.MustParse("2Gi"),
										corev1.ResourceCPU:    resource.MustParse("4"),
									},
								},
							},
							Storage: multigresv1alpha1.StorageSpec{Size: "5Gi"},
						},
					},
				},
			}
			mem, cpu, disk := reduceShardResources(shard)
			c.Eq(2*(1<<30), mem, "mem")
			c.Eq(4000, cpu, "cpu")
			c.Eq(5*(1<<30), disk, "disk")
		},
	)

	t.Run("no pools returns zeros", func(t *testing.T) {
		mem, cpu, disk := reduceShardResources(&multigresv1alpha1.Shard{})
		assert.NewCollecting(t).
			False(mem != 0 || cpu != 0 || disk != 0, "got (%d, %d, %d), want all zero", mem, cpu, disk)
	})

	t.Run("pools without resources return zeros", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{Spec: multigresv1alpha1.ShardSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"a": {}},
		}}
		mem, cpu, disk := reduceShardResources(shard)
		assert.NewCollecting(t).
			False(mem != 0 || cpu != 0 || disk != 0, "got (%d, %d, %d), want all zero", mem, cpu, disk)
	})
}

// A shard with no PostgresConfigRef still gets an operator-owned ConfigMap
// rendering the baseline — the operator always owns the file.
func TestReconcilePostgresConfig_RendersBaselineWithoutRef(t *testing.T) {
	ck := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "default"},
		Spec:       multigresv1alpha1.ShardSpec{},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme}

	cfg := r.renderEffectiveConfig(context.Background(), shard)
	ck.Require().
		NoError(r.reconcilePostgresConfig(context.Background(), shard, cfg), "reconcilePostgresConfig() error =")

	// The operator ConfigMap must exist with the rendered baseline.
	got := &corev1.ConfigMap{}
	ck.Require().NoError(c.Get(context.Background(), client.ObjectKey{
		Namespace: "default",
		Name:      PostgresConfigMapName("s1"),
	}, got), "operator ConfigMap not created")
	rendered := got.Data[PostgresConfigMapKey]
	ck.StrContains(
		rendered,
		"shared_buffers = 64MB",
		"rendered baseline missing default shared_buffers:\n",
	)

	// The content hash annotation must be stamped.
	ck.Len(shard.Annotations[metadata.AnnotationPostgresConfigHash], 64, "hash annotation")
}

// A shard with a PostgresConfigRef renders the baseline plus the ref content
// into the operator ConfigMap.
func TestReconcilePostgresConfig_MergesRefContent(t *testing.T) {
	ck := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	userCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "user-cm", Namespace: "default"},
		Data:       map[string]string{"custom.conf": "shared_buffers = '8GB'"},
	}
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "default"},
		Spec: multigresv1alpha1.ShardSpec{
			PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
				Name: "user-cm",
				Key:  "custom.conf",
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(userCM).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme}

	cfg := r.renderEffectiveConfig(context.Background(), shard)
	ck.Require().
		NoError(r.reconcilePostgresConfig(context.Background(), shard, cfg), "reconcilePostgresConfig() error =")

	got := &corev1.ConfigMap{}
	ck.Require().NoError(c.Get(context.Background(), client.ObjectKey{
		Namespace: "default",
		Name:      PostgresConfigMapName("s1"),
	}, got), "operator ConfigMap not created")
	rendered := got.Data[PostgresConfigMapKey]
	// Baseline present, and the ref rendered BEFORE it so the operator's
	// resource-derived baseline wins last-write-wins: the baseline's
	// shared_buffers = 64MB must override the ref's 8GB. The deprecated ref must
	// not override the operator's sizing math — only inline spec.postgresConfig.
	ck.StrContains(rendered, "shared_buffers = 64MB", "rendered config missing baseline:\n")
	ck.StrContains(rendered, "shared_buffers = '8GB'", "rendered config missing ref content:\n")
	ck.LessOrEqual(strings.Index(
		rendered,
		"shared_buffers = 64MB",
	), strings.Index(
		rendered,
		"shared_buffers = '8GB'",
	), "ref content should precede the baseline so the baseline wins:\n%s", rendered)
}

// The inline spec.postgresConfig map is rendered last, so it overrides both the
// baseline and any PostgresConfigRef content.
func TestReconcilePostgresConfig_InlineMapWins(t *testing.T) {
	ck := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	userCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "user-cm", Namespace: "default"},
		Data:       map[string]string{"custom.conf": "work_mem = '1MB'"},
	}
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "default"},
		Spec: multigresv1alpha1.ShardSpec{
			PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
				Name: "user-cm",
				Key:  "custom.conf",
			},
			PostgresConfig: map[string]string{"work_mem": "64MB"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(userCM).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme}

	cfg := r.renderEffectiveConfig(context.Background(), shard)
	ck.Require().
		NoError(r.reconcilePostgresConfig(context.Background(), shard, cfg), "reconcilePostgresConfig() error =")

	got := &corev1.ConfigMap{}
	ck.Require().NoError(c.Get(context.Background(), client.ObjectKey{
		Namespace: "default",
		Name:      PostgresConfigMapName("s1"),
	}, got), "operator ConfigMap not created")
	rendered := got.Data[PostgresConfigMapKey]
	ck.StrContains(rendered, "work_mem = '64MB'", "rendered config missing inline override:\n")
	// The inline map must appear after the ref content so it wins last-write-wins.
	ck.GreaterOrEqual(
		strings.Index(rendered, "work_mem = '1MB'"),
		strings.Index(rendered, "work_mem = '64MB'"),
		"inline map should follow the ref content:\n%s",
		rendered,
	)
}
