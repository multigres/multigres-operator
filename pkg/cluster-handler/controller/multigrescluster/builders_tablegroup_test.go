package multigrescluster

import (
	"strings"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	"github.com/multigres/multigres-operator/pkg/util/name"

	"github.com/multigres/testkit/assert"
)

func TestBuildTableGroup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
	}

	dbCfg := multigresv1alpha1.DatabaseConfig{
		Name: "my-db",
	}
	globalTopoRef := multigresv1alpha1.GlobalTopoServerRef{}

	t.Run("Success", func(t *testing.T) {
		c := assert.NewCollecting(t)
		tgCfg := &multigresv1alpha1.TableGroupConfig{
			Name: "tg-1",
		}
		resolvedShards := []multigresv1alpha1.ShardResolvedSpec{
			{Name: "shard-0"},
		}

		got, err := BuildTableGroup(cluster, dbCfg, tgCfg, resolvedShards, globalTopoRef, scheme)
		c.Require().NoError(err, "BuildTableGroup() error =")

		// Calculate expected hash: md5("my-cluster", "my-db", "tg-1") -> "d5708433"
		expectedName := name.JoinWithConstraints(
			name.DefaultConstraints,
			"my-cluster",
			"my-db",
			"tg-1",
		)
		c.Eq(expectedName, got.Name, "Name")
		c.Eq("my-db", got.Labels["multigres.com/database"], "Label[database]")

		// Verify OwnerReference
		c.Len(
			got.OwnerReferences,
			1,
			"OwnerReferences count = %v, want 1",
			len(got.OwnerReferences),
		)
	})

	t.Run("CustomPostgresSuperuser", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		c := *cluster
		c.Spec.PostgresSuperuser = "admin"
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg-superuser"}
		got, err := BuildTableGroup(&c, dbCfg, tgCfg, nil, globalTopoRef, scheme)
		ck.Require().NoError(err, "BuildTableGroup() error =")
		ck.Eq("admin", got.Spec.PostgresSuperuser, "PostgresSuperuser")
	})

	t.Run("Propagates InternalTLS", func(t *testing.T) {
		ck := assert.NewAborting(t)
		c := cluster.DeepCopy()
		c.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)}
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg-internal-tls"}

		got, err := BuildTableGroup(c, dbCfg, tgCfg, nil, globalTopoRef, scheme)
		ck.NoError(err, "BuildTableGroup() error =")
		ck.Eq(c.Spec.InternalTLS, got.Spec.InternalTLS, "InternalTLS")
	})

	t.Run("PostgresPasswordSecretRef", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		c := *cluster
		c.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
			Name: "multigres-admin-password",
			Key:  "current",
		}
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg-password"}
		got, err := BuildTableGroup(&c, dbCfg, tgCfg, nil, globalTopoRef, scheme)
		ck.Require().NoError(err, "BuildTableGroup() error =")
		ck.Eq(
			"multigres-admin-password",
			got.Spec.PostgresPasswordSecretRef.Name,
			"PostgresPasswordSecretRef.Name",
		)
		ck.Eq("current", got.Spec.PostgresPasswordSecretRef.Key, "PostgresPasswordSecretRef.Key")
	})

	t.Run("PostgresInitSecretsRef propagated when set", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		c := *cluster
		c.Spec.PostgresInitSecretsRef = &multigresv1alpha1.PostgresInitSecretsRef{
			Name: "multigres-init-secrets",
			Key:  "init-secrets.json",
		}
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg-init-secrets"}
		got, err := BuildTableGroup(&c, dbCfg, tgCfg, nil, globalTopoRef, scheme)
		ck.Require().NoError(err, "BuildTableGroup() error =")
		ck.Require().
			NotNil(got.Spec.PostgresInitSecretsRef, "PostgresInitSecretsRef = nil, want propagated ref")
		ck.Eq(
			"multigres-init-secrets",
			got.Spec.PostgresInitSecretsRef.Name,
			"PostgresInitSecretsRef.Name",
		)
		ck.Eq(
			"init-secrets.json",
			got.Spec.PostgresInitSecretsRef.Key,
			"PostgresInitSecretsRef.Key",
		)
	})

	t.Run("PostgresInitSecretsRef nil when unset", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		c := *cluster
		c.Spec.PostgresInitSecretsRef = nil
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg-no-init-secrets"}
		got, err := BuildTableGroup(&c, dbCfg, tgCfg, nil, globalTopoRef, scheme)
		ck.Require().NoError(err, "BuildTableGroup() error =")
		ck.Nil(got.Spec.PostgresInitSecretsRef, "PostgresInitSecretsRef")
	})

	t.Run("Name Truncation", func(t *testing.T) {
		c := assert.NewCollecting(t)
		longName := strings.Repeat("a", 250) // Very long name
		tgCfg := &multigresv1alpha1.TableGroupConfig{
			Name: multigresv1alpha1.TableGroupName(longName),
		}
		resolvedShards := []multigresv1alpha1.ShardResolvedSpec{}

		got, err := BuildTableGroup(cluster, dbCfg, tgCfg, resolvedShards, globalTopoRef, scheme)
		c.NoError(err, "BuildTableGroup() error")
		// Should be truncated to 253 chars
		c.LessOrEqual(253, len(got.Name), "Expected name length <= 253, got")
		// Confirm it ends with a hash (8 chars)
		// and has the truncation mark "---"
		c.StrContains(got.Name, "---", "Expected truncation mark '---', got")
	})

	t.Run("CellTopologyLabels ZoneID", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		c := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-cluster",
				Namespace: "default",
				UID:       "cluster-uid",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{{Name: "az-cell", ZoneID: "use1-az1"}},
			},
		}
		got, err := BuildTableGroup(
			c,
			dbCfg,
			&multigresv1alpha1.TableGroupConfig{Name: "tg"},
			[]multigresv1alpha1.ShardResolvedSpec{{Name: "s0"}},
			globalTopoRef,
			scheme,
		)
		ck.Require().NoError(err, "BuildTableGroup() error =")
		ck.Eq(
			"use1-az1",
			got.Spec.CellTopologyLabels["az-cell"]["topology.k8s.aws/zone-id"],
			"expected topology.k8s.aws/zone-id=use1-az1, got %v",
			got.Spec.CellTopologyLabels["az-cell"],
		)
	})

	t.Run("CellTopologyLabels ZoneID only", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		c := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-cluster",
				Namespace: "default",
				UID:       "cluster-uid",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "both-cell", ZoneID: "use1-az1"},
				},
			},
		}
		got, err := BuildTableGroup(
			c,
			dbCfg,
			&multigresv1alpha1.TableGroupConfig{Name: "tg"},
			[]multigresv1alpha1.ShardResolvedSpec{{Name: "s0"}},
			globalTopoRef,
			scheme,
		)
		ck.Require().NoError(err, "BuildTableGroup() error =")
		labels := got.Spec.CellTopologyLabels["both-cell"]
		ck.Eq(
			"use1-az1",
			labels["topology.k8s.aws/zone-id"],
			"expected topology.k8s.aws/zone-id=use1-az1, got %v",
			labels,
		)
	})

	t.Run("CellTopologyLabels Region", func(t *testing.T) {
		c := assert.NewCollecting(t)
		regionCluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-cluster",
				Namespace: "default",
				UID:       "cluster-uid",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "region-cell", Region: "us-east-1"},
				},
			},
		}
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg-region"}
		resolvedShards := []multigresv1alpha1.ShardResolvedSpec{{Name: "shard-0"}}

		got, err := BuildTableGroup(
			regionCluster,
			dbCfg,
			tgCfg,
			resolvedShards,
			globalTopoRef,
			scheme,
		)
		c.Require().NoError(err, "BuildTableGroup() error =")
		labels, ok := got.Spec.CellTopologyLabels["region-cell"]
		c.Require().True(ok, "Expected CellTopologyLabels to contain region-cell")
		c.Eq(
			"us-east-1",
			labels["topology.kubernetes.io/region"],
			"Expected region label us-east-1, got",
		)
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg1"}
		resolvedShards := []multigresv1alpha1.ShardResolvedSpec{}
		emptyScheme := runtime.NewScheme()

		_, err := BuildTableGroup(
			cluster,
			dbCfg,
			tgCfg,
			resolvedShards,
			globalTopoRef,
			emptyScheme,
		)
		assert.NewCollecting(t).Error(err, "Expected error due to missing scheme types, got nil")
	})

	t.Run("Propagates explicit project ref annotation", func(t *testing.T) {
		c := assert.NewAborting(t)
		clusterWithProjectRef := cluster.DeepCopy()
		clusterWithProjectRef.Annotations = map[string]string{
			metadata.AnnotationProjectRef: "proj_123",
		}
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg-with-project-ref"}
		resolvedShards := []multigresv1alpha1.ShardResolvedSpec{{Name: "shard-0"}}

		got, err := BuildTableGroup(
			clusterWithProjectRef,
			dbCfg,
			tgCfg,
			resolvedShards,
			globalTopoRef,
			scheme,
		)
		c.NoError(err, "BuildTableGroup() error =")

		c.Eq(
			"proj_123",
			got.Annotations[metadata.AnnotationProjectRef],
			"annotation %q = %q, want",
			metadata.AnnotationProjectRef,
			got.Annotations[metadata.AnnotationProjectRef],
		)
	})
}
