package shard

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestBuildPostgresPasswordSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	tests := map[string]struct {
		shard   *multigresv1alpha1.Shard
		scheme  *runtime.Scheme
		want    *corev1.Secret
		wantErr bool
	}{
		"builds secret with correct metadata and data": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
					UID:       "test-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
			},
			scheme: scheme,
			want: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      PostgresPasswordSecretName("test-shard"),
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "postgres-password",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "test-cluster",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Shard",
							Name:               "test-shard",
							UID:                "test-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Data: map[string][]byte{
					PostgresPasswordSecretKey: []byte(DefaultPostgresPassword),
				},
			},
		},
		"error with empty scheme": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
					UID:       "test-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
			},
			scheme:  runtime.NewScheme(),
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := BuildPostgresPasswordSecret(tc.shard, tc.scheme)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("BuildPostgresPasswordSecret() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReconcilePostgresPasswordSecret_SkipsExternalRef(t *testing.T) {
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
			PostgresPasswordSecretRef: &multigresv1alpha1.PostgresPasswordSecretRef{
				Name: "multigres-admin-password",
				Key:  "current",
			},
		},
	}

	externalSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "multigres-admin-password",
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

	secret := &corev1.Secret{}
	err := client.Get(
		context.Background(),
		types.NamespacedName{Name: PostgresPasswordSecretName("test-shard"), Namespace: "default"},
		secret,
	)
	if !apierrors.IsNotFound(err) {
		t.Fatalf(
			"default postgres password Secret should not be created, got err %v and secret %#v",
			err,
			secret,
		)
	}
}
