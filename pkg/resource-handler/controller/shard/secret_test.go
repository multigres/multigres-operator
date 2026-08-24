package shard

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

const testPostgresAuthRefName = "multigres-admin-ref"

//nolint:gosec // Test name only: not a credential.
const testPostgresInitSecretsRefName = "multigres-init-secrets-ref"

func setTestPostgresPasswordSecretRef(shard *multigresv1alpha1.Shard) {
	shard.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
		Name: testPostgresAuthRefName,
		Key:  PostgresPasswordSecretKey,
	}
}

func setTestPostgresInitSecretsRef(shard *multigresv1alpha1.Shard) {
	shard.Spec.PostgresInitSecretsRef = &multigresv1alpha1.PostgresInitSecretsRef{
		Name: testPostgresInitSecretsRefName,
		Key:  PostgresInitSecretsFileName,
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

func TestReconcilePostgresInitSecretsSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	const payloadRoleName = "super-secret-role-name"
	const payloadPassword = "super-secret-password-value"

	newShard := func() *multigresv1alpha1.Shard {
		return &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-shard",
				Namespace: "default",
				UID:       "test-uid",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
		}
	}

	tests := map[string]struct {
		configureShard func(shard *multigresv1alpha1.Shard)
		secret         *corev1.Secret
		wantErr        bool
		wantErrText    string
	}{
		"nil ref": {
			configureShard: func(*multigresv1alpha1.Shard) {},
			wantErr:        false,
		},
		"missing secret": {
			configureShard: setTestPostgresInitSecretsRef,
			secret:         nil,
			wantErr:        true,
		},
		"missing key": {
			configureShard: setTestPostgresInitSecretsRef,
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPostgresInitSecretsRefName,
					Namespace: "default",
				},
				Data: map[string][]byte{"other-key": []byte(`{}`)},
			},
			wantErr: true,
		},
		"empty value": {
			configureShard: setTestPostgresInitSecretsRef,
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPostgresInitSecretsRefName,
					Namespace: "default",
				},
				Data: map[string][]byte{PostgresInitSecretsFileName: []byte("")},
			},
			wantErr: true,
		},
		"invalid JSON": {
			configureShard: setTestPostgresInitSecretsRef,
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPostgresInitSecretsRefName,
					Namespace: "default",
				},
				Data: map[string][]byte{PostgresInitSecretsFileName: []byte("{not json")},
			},
			wantErr:     true,
			wantErrText: "not valid JSON",
		},
		"null JSON payload": {
			configureShard: setTestPostgresInitSecretsRef,
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPostgresInitSecretsRefName,
					Namespace: "default",
				},
				Data: map[string][]byte{PostgresInitSecretsFileName: []byte(`null`)},
			},
			wantErr:     true,
			wantErrText: "must be a JSON object, not null",
		},
		"database settings with numeric value": {
			configureShard: setTestPostgresInitSecretsRef,
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPostgresInitSecretsRefName,
					Namespace: "default",
				},
				Data: map[string][]byte{
					PostgresInitSecretsFileName: []byte(
						`{"roles":{"` + payloadRoleName + `":"` + payloadPassword + `"},` +
							`"database_settings":{"postgres":{"max_connections":100}}}`,
					),
				},
			},
			wantErr:     true,
			wantErrText: "string values in roles and database_settings",
		},
		"valid JSON": {
			configureShard: setTestPostgresInitSecretsRef,
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPostgresInitSecretsRefName,
					Namespace: "default",
				},
				Data: map[string][]byte{
					PostgresInitSecretsFileName: []byte(`{
						"roles": {"` + payloadRoleName + `": "` + payloadPassword + `"},
						"database_settings": {"mydb": {"work_mem": "64MB"}}
					}`),
				},
			},
			wantErr: false,
		},
		"valid empty JSON object": {
			configureShard: setTestPostgresInitSecretsRef,
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPostgresInitSecretsRefName,
					Namespace: "default",
				},
				Data: map[string][]byte{PostgresInitSecretsFileName: []byte(`{}`)},
			},
			wantErr: false,
		},
		"valid JSON with roles only": {
			configureShard: setTestPostgresInitSecretsRef,
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPostgresInitSecretsRefName,
					Namespace: "default",
				},
				Data: map[string][]byte{
					PostgresInitSecretsFileName: []byte(`{"roles":{"app":"password"}}`),
				},
			},
			wantErr: false,
		},
		"valid JSON with unknown top-level fields": {
			configureShard: setTestPostgresInitSecretsRef,
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPostgresInitSecretsRefName,
					Namespace: "default",
				},
				Data: map[string][]byte{
					PostgresInitSecretsFileName: []byte(
						`{"roles": {}, "database_settings": {}, "unexpected_field": "value"}`,
					),
				},
			},
			wantErr: false,
		},
		"valid JSON with quoted numeric database setting": {
			configureShard: setTestPostgresInitSecretsRef,
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPostgresInitSecretsRefName,
					Namespace: "default",
				},
				Data: map[string][]byte{
					PostgresInitSecretsFileName: []byte(
						`{"database_settings":{"postgres":{"max_connections":"100"}}}`,
					),
				},
			},
			wantErr: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			shard := newShard()
			tc.configureShard(shard)

			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.secret != nil {
				builder = builder.WithObjects(tc.secret)
			}
			client := builder.Build()
			reconciler := &ShardReconciler{
				Client: client,
				Scheme: scheme,
			}

			err := reconciler.reconcilePostgresInitSecretsSecret(context.Background(), shard)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err != nil {
				msg := err.Error()
				if tc.wantErrText != "" && !strings.Contains(msg, tc.wantErrText) {
					t.Errorf("error = %q, want it to contain %q", msg, tc.wantErrText)
				}
				if strings.Contains(msg, payloadRoleName) {
					t.Errorf("error message must not contain role names, got: %q", msg)
				}
				if strings.Contains(msg, payloadPassword) {
					t.Errorf("error message must not contain payload values, got: %q", msg)
				}
			}
		})
	}
}
