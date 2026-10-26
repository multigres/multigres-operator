package observer

import (
	"context"
	"log/slog"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/tools/observer/pkg/common"
)

func TestFetchShardPassword(t *testing.T) {
	ctx := context.Background()
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "test-ns",
		},
	}
	unstructuredShard := testUnstructuredShard(
		"test-shard",
		"test-ns",
		"cluster-admin-password",
		"current",
	)

	t.Run("reads referenced secret key", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cluster-admin-password",
				Namespace: "test-ns",
			},
			Data: map[string][]byte{
				"current": []byte("secret-password"),
			},
		}

		o := newConnectivityTestObserver(unstructuredShard, secret)
		got := o.fetchShardPassword(ctx, shard)
		if got != "secret-password" {
			t.Fatalf("fetchShardPassword() = %q, want %q", got, "secret-password")
		}
	})

	t.Run("missing secret returns empty password", func(t *testing.T) {
		o := newConnectivityTestObserver(unstructuredShard)
		got := o.fetchShardPassword(ctx, shard)
		if got != "" {
			t.Fatalf("fetchShardPassword() = %q, want empty string", got)
		}
	})

	t.Run("missing key returns empty password", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cluster-admin-password",
				Namespace: "test-ns",
			},
			Data: map[string][]byte{
				"password": []byte("legacy-password"),
			},
		}

		o := newConnectivityTestObserver(unstructuredShard, secret)
		got := o.fetchShardPassword(ctx, shard)
		if got != "" {
			t.Fatalf("fetchShardPassword() = %q, want empty string", got)
		}
	})
}

func newConnectivityTestObserver(objects ...runtime.Object) *Observer {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   "multigres.com",
		Version: "v1alpha1",
		Kind:    "Shard",
	}, &unstructured.Unstructured{})

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return &Observer{
		client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithRuntimeObjects(objects...).
			Build(),
		logger: logger,
	}
}

func testUnstructuredShard(name, namespace, secretName, secretKey string) runtime.Object {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "multigres.com/v1alpha1",
			"kind":       "Shard",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"postgresPasswordSecretRef": map[string]interface{}{
					"name": secretName,
					"key":  secretKey,
				},
			},
		},
	}
}

func TestMultigatewayPGPort(t *testing.T) {
	svcWithPorts := func(ports ...int32) *corev1.Service {
		svc := &corev1.Service{}
		for _, p := range ports {
			svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{Port: p})
		}
		return svc
	}

	tests := map[string]struct {
		svc  *corev1.Service
		want int
	}{
		"primary service exposes 5432": {
			svc:  svcWithPorts(5432),
			want: common.PortMultigatewayPG,
		},
		"per-cell service exposes 5432 with HTTP and gRPC": {
			svc:  svcWithPorts(15100, 15170, 5432),
			want: common.PortMultigatewayPG,
		},
		"replica service exposes only 5433": {
			svc:  svcWithPorts(5433),
			want: common.PortMultigatewayReplicaPG,
		},
		"service exposes both ports": {
			svc:  svcWithPorts(5432, 5433),
			want: common.PortMultigatewayPG,
		},
		"service exposes neither port": {
			svc:  svcWithPorts(15100),
			want: common.PortMultigatewayPG,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := multigatewayPGPort(tc.svc); got != tc.want {
				t.Errorf("multigatewayPGPort() = %d, want %d", got, tc.want)
			}
		})
	}
}
