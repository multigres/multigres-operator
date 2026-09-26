package tablegroup

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"

	"github.com/multigres/testkit/assert"
)

func TestBuildShard(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-tg",
			Namespace: "default",
			UID:       "tg-uid",
			Labels: map[string]string{
				"multigres.com/cluster":  "my-cluster",
				"multigres.com/database": "my-db",
			},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   "my-db",
			TableGroupName: "my-tg",
		},
	}

	shardSpec := &multigresv1alpha1.ShardResolvedSpec{
		Name: "shard-0",
		Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
			"default": {Type: "transaction"},
		},
	}

	t.Run("Success", func(t *testing.T) {
		c := assert.NewCollecting(t)
		got, err := BuildShard(tg, shardSpec, scheme)
		c.Require().NoError(err, "BuildShard() error =")

		// Calculate expected hash: md5("my-cluster", "my-db", "my-tg", "shard-0") -> "a068d59f"
		expectedName := name.JoinWithConstraints(
			name.DefaultConstraints,
			"my-cluster",
			"my-db",
			"my-tg",
			"shard-0",
		)
		c.Eq(expectedName, got.Name, "Name")
		c.Eq("default", got.Namespace, "Namespace")
		c.Eq("my-cluster", got.Labels["multigres.com/cluster"], "Labels[cluster]")
		// Verify OwnerReference pointing to TableGroup
		if len(got.OwnerReferences) != 1 {
			t.Errorf("OwnerReferences count = %v, want 1", len(got.OwnerReferences))
		} else {
			c.Eq("my-tg", got.OwnerReferences[0].Name, "OwnerReference Name")
			c.Eq("tg-uid", got.OwnerReferences[0].UID, "OwnerReference UID")
		}

		// Verify Spec fields are copied
		c.Eq("shard-0", got.Spec.ShardName, "Spec.ShardName")
		c.Eq("my-db", got.Spec.DatabaseName, "Spec.DatabaseName")
		c.EqDiff(shardSpec.Pools, got.Spec.Pools, "Spec.Pools mismatch")
	})

	t.Run("DurabilityPolicy propagates from TableGroup to Shard", func(t *testing.T) {
		c := assert.NewCollecting(t)
		tgWithPolicy := tg.DeepCopy()
		tgWithPolicy.Spec.DurabilityPolicy = "MULTI_CELL_AT_LEAST_2"

		got, err := BuildShard(tgWithPolicy, shardSpec, scheme)
		c.Require().NoError(err, "BuildShard() error =")
		c.Eq("MULTI_CELL_AT_LEAST_2", got.Spec.DurabilityPolicy, "Spec.DurabilityPolicy")
	})

	t.Run("InternalTLS propagates from TableGroup to Shard", func(t *testing.T) {
		c := assert.NewAborting(t)
		tgWithInternalTLS := tg.DeepCopy()
		tgWithInternalTLS.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{
			Enabled: ptr.To(true),
		}

		got, err := BuildShard(tgWithInternalTLS, shardSpec, scheme)
		c.NoError(err, "BuildShard() error =")
		c.Eq(tgWithInternalTLS.Spec.InternalTLS, got.Spec.InternalTLS, "Spec.InternalTLS")
	})

	t.Run("PostgresPasswordSecretRef propagates from TableGroup to Shard", func(t *testing.T) {
		c := assert.NewCollecting(t)
		tgWithRef := tg.DeepCopy()
		tgWithRef.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
			Name: "multigres-admin-password",
			Key:  "current",
		}

		got, err := BuildShard(tgWithRef, shardSpec, scheme)
		c.Require().NoError(err, "BuildShard() error =")
		c.Eq(
			"multigres-admin-password",
			got.Spec.PostgresPasswordSecretRef.Name,
			"Spec.PostgresPasswordSecretRef.Name",
		)
		c.Eq(
			"current",
			got.Spec.PostgresPasswordSecretRef.Key,
			"Spec.PostgresPasswordSecretRef.Key",
		)
	})

	t.Run(
		"PostgresInitSecretsRef propagates from TableGroup to Shard when set",
		func(t *testing.T) {
			c := assert.NewCollecting(t)
			tgWithRef := tg.DeepCopy()
			tgWithRef.Spec.PostgresInitSecretsRef = &multigresv1alpha1.PostgresInitSecretsRef{
				Name: "multigres-init-secrets",
				Key:  "init-secrets.json",
			}

			got, err := BuildShard(tgWithRef, shardSpec, scheme)
			c.Require().NoError(err, "BuildShard() error =")
			c.Require().
				NotNil(got.Spec.PostgresInitSecretsRef, "Spec.PostgresInitSecretsRef = nil, want propagated ref")
			c.Eq(
				"multigres-init-secrets",
				got.Spec.PostgresInitSecretsRef.Name,
				"Spec.PostgresInitSecretsRef.Name",
			)
			c.Eq(
				"init-secrets.json",
				got.Spec.PostgresInitSecretsRef.Key,
				"Spec.PostgresInitSecretsRef.Key",
			)
		},
	)

	t.Run("PostgresInitSecretsRef nil on Shard when unset on TableGroup", func(t *testing.T) {
		c := assert.NewCollecting(t)
		tgWithoutRef := tg.DeepCopy()
		tgWithoutRef.Spec.PostgresInitSecretsRef = nil

		got, err := BuildShard(tgWithoutRef, shardSpec, scheme)
		c.Require().NoError(err, "BuildShard() error =")
		c.Nil(got.Spec.PostgresInitSecretsRef, "Spec.PostgresInitSecretsRef")
	})

	t.Run("DurabilityPolicy empty when not set on TableGroup", func(t *testing.T) {
		c := assert.NewCollecting(t)
		got, err := BuildShard(tg, shardSpec, scheme)
		c.Require().NoError(err, "BuildShard() error =")
		c.Eq("", got.Spec.DurabilityPolicy, "Spec.DurabilityPolicy")
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		// Intentionally missing scheme registrations to force SetControllerReference failure
		_, err := BuildShard(tg, shardSpec, emptyScheme)
		assert.NewCollecting(t).Error(err, "Expected error due to missing scheme types, got nil")
	})

	t.Run("Propagates explicit project ref annotation", func(t *testing.T) {
		c := assert.NewAborting(t)
		tgWithProjectRef := tg.DeepCopy()
		tgWithProjectRef.Annotations = map[string]string{
			metadata.AnnotationProjectRef: "proj_123",
		}

		got, err := BuildShard(tgWithProjectRef, shardSpec, scheme)
		c.NoError(err, "BuildShard() error =")

		c.Eq(
			"proj_123",
			got.Annotations[metadata.AnnotationProjectRef],
			"annotation %q = %q, want",
			metadata.AnnotationProjectRef,
			got.Annotations[metadata.AnnotationProjectRef],
		)
	})
}
