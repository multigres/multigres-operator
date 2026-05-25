package shard

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

const testPostgresAuthRefName = "multigres-admin-ref"

func setTestPostgresPasswordSecretRef(shard *multigresv1alpha1.Shard) {
	shard.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
		Name: testPostgresAuthRefName,
		Key:  PostgresPasswordSecretKey,
	}
}

func testPostgresPasswordSecretForShard(shard *multigresv1alpha1.Shard) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPostgresAuthRefName,
			Namespace: shard.Namespace,
		},
		Data: map[string][]byte{
			PostgresPasswordSecretKey: []byte("secret-password"),
		},
	}
}

func TestReconcilePostgresPasswordSecret_ValidatesExternalRef(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			UID:       "test-uid",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: testPostgresAuthRefName,
				Key:  "current",
			},
		},
	}

	externalSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPostgresAuthRefName,
			Namespace: "default",
		},
		Data: map[string][]byte{"current": []byte("secret-password")},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(externalSecret).Build()
	reconciler := &ShardReconciler{
		Client: client,
		Scheme: scheme,
	}

	if err := reconciler.reconcilePostgresPasswordSecret(context.Background(), shard); err != nil {
		t.Fatalf("reconcilePostgresPasswordSecret() error = %v", err)
	}
}
