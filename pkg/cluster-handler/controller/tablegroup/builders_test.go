package tablegroup

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
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
		got, err := BuildShard(tg, shardSpec, scheme)
		if err != nil {
			t.Fatalf("BuildShard() error = %v", err)
		}

		// Calculate expected hash: md5("my-cluster", "my-db", "my-tg", "shard-0") -> "a068d59f"
		expectedName := name.JoinWithConstraints(
			name.DefaultConstraints,
			"my-cluster",
			"my-db",
			"my-tg",
			"shard-0",
		)
		if got.Name != expectedName {
			t.Errorf("Name = %v, want %v", got.Name, expectedName)
		}
		if got.Namespace != "default" {
			t.Errorf("Namespace = %v, want %v", got.Namespace, "default")
		}
		if got.Labels["multigres.com/cluster"] != "my-cluster" {
			t.Errorf(
				"Labels[cluster] = %v, want %v",
				got.Labels["multigres.com/cluster"],
				"my-cluster",
			)
		}
		// Verify OwnerReference pointing to TableGroup
		if len(got.OwnerReferences) != 1 {
			t.Errorf("OwnerReferences count = %v, want 1", len(got.OwnerReferences))
		} else {
			if got.OwnerReferences[0].Name != "my-tg" {
				t.Errorf("OwnerReference Name = %v, want %v", got.OwnerReferences[0].Name, "my-tg")
			}
			if got.OwnerReferences[0].UID != "tg-uid" {
				t.Errorf("OwnerReference UID = %v, want %v", got.OwnerReferences[0].UID, "tg-uid")
			}
		}

		// Verify Spec fields are copied
		if got.Spec.ShardName != "shard-0" {
			t.Errorf("Spec.ShardName = %v, want %v", got.Spec.ShardName, "shard-0")
		}
		if got.Spec.DatabaseName != "my-db" {
			t.Errorf("Spec.DatabaseName = %v, want %v", got.Spec.DatabaseName, "my-db")
		}
		if diff := cmp.Diff(shardSpec.Pools, got.Spec.Pools); diff != "" {
			t.Errorf("Spec.Pools mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("DurabilityPolicy propagates from TableGroup to Shard", func(t *testing.T) {
		tgWithPolicy := tg.DeepCopy()
		tgWithPolicy.Spec.DurabilityPolicy = "MULTI_CELL_AT_LEAST_2"

		got, err := BuildShard(tgWithPolicy, shardSpec, scheme)
		if err != nil {
			t.Fatalf("BuildShard() error = %v", err)
		}
		if got.Spec.DurabilityPolicy != "MULTI_CELL_AT_LEAST_2" {
			t.Errorf(
				"Spec.DurabilityPolicy = %v, want MULTI_CELL_AT_LEAST_2",
				got.Spec.DurabilityPolicy,
			)
		}
	})

	t.Run("PostgresPasswordSecretRef propagates from TableGroup to Shard", func(t *testing.T) {
		tgWithRef := tg.DeepCopy()
		tgWithRef.Spec.PostgresPasswordSecretRef = &multigresv1alpha1.PostgresPasswordSecretRef{
			Name: "multigres-admin-password",
			Key:  "current",
		}

		got, err := BuildShard(tgWithRef, shardSpec, scheme)
		if err != nil {
			t.Fatalf("BuildShard() error = %v", err)
		}
		if got.Spec.PostgresPasswordSecretRef == nil {
			t.Fatal("Spec.PostgresPasswordSecretRef = nil, want propagated ref")
		}
		if got.Spec.PostgresPasswordSecretRef.Name != "multigres-admin-password" {
			t.Errorf(
				"Spec.PostgresPasswordSecretRef.Name = %v, want multigres-admin-password",
				got.Spec.PostgresPasswordSecretRef.Name,
			)
		}
		if got.Spec.PostgresPasswordSecretRef.Key != "current" {
			t.Errorf("Spec.PostgresPasswordSecretRef.Key = %v, want current", got.Spec.PostgresPasswordSecretRef.Key)
		}
	})

	t.Run("DurabilityPolicy empty when not set on TableGroup", func(t *testing.T) {
		got, err := BuildShard(tg, shardSpec, scheme)
		if err != nil {
			t.Fatalf("BuildShard() error = %v", err)
		}
		if got.Spec.DurabilityPolicy != "" {
			t.Errorf("Spec.DurabilityPolicy = %v, want empty", got.Spec.DurabilityPolicy)
		}
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		// Intentionally missing scheme registrations to force SetControllerReference failure
		_, err := BuildShard(tg, shardSpec, emptyScheme)
		if err == nil {
			t.Error("Expected error due to missing scheme types, got nil")
		}
	})

	t.Run("Propagates explicit project ref annotation", func(t *testing.T) {
		tgWithProjectRef := tg.DeepCopy()
		tgWithProjectRef.Annotations = map[string]string{
			metadata.AnnotationProjectRef: "proj_123",
		}

		got, err := BuildShard(tgWithProjectRef, shardSpec, scheme)
		if err != nil {
			t.Fatalf("BuildShard() error = %v", err)
		}

		if got.Annotations[metadata.AnnotationProjectRef] != "proj_123" {
			t.Fatalf(
				"annotation %q = %q, want %q",
				metadata.AnnotationProjectRef,
				got.Annotations[metadata.AnnotationProjectRef],
				"proj_123",
			)
		}
	})
}
