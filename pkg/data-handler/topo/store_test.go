package topo_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	_ "github.com/multigres/multigres/go/common/topoclient/etcdtopo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
)

func topoTestClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func TestNewStoreFromShard(t *testing.T) {
	tests := map[string]struct {
		shard   *multigresv1alpha1.Shard
		wantErr bool
	}{
		"creates store for etcd": {
			shard: &multigresv1alpha1.Shard{
				Spec: multigresv1alpha1.ShardSpec{
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:  "localhost:2379",
						RootPath: "/test",
					},
				},
			},
			wantErr: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			store, err := topo.NewStoreFromShard(context.Background(), topoTestClient(), tc.shard)
			if (err != nil) != tc.wantErr {
				t.Errorf("NewStoreFromShard() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !tc.wantErr && store != nil {
				_ = store.Close()
			}
		})
	}
}

func TestNewStoreFromShard_InvalidImplementation(t *testing.T) {
	shard := &multigresv1alpha1.Shard{
		Spec: multigresv1alpha1.ShardSpec{
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "localhost:2379",
				RootPath:       "/test",
				Implementation: "invalid-implementation-that-does-not-exist",
			},
		},
	}

	store, err := topo.NewStoreFromShard(context.Background(), topoTestClient(), shard)
	if err == nil {
		if store != nil {
			_ = store.Close()
		}
		t.Error("NewStoreFromShard() should error with invalid implementation")
	}
}

// A reference that names a client credential Secret which does not exist has to
// fail with the Secret name, so a misconfigured cluster reports the missing
// material instead of silently connecting without a certificate.
func TestNewStoreFromShard_MissingSecretIsLoud(t *testing.T) {
	tlsName := "cluster-topo-client-tls"
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a"},
		Spec: multigresv1alpha1.ShardSpec{
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:          "localhost:2379",
				RootPath:         "/test",
				CASecret:         tlsName,
				ClientCertSecret: tlsName,
			},
		},
	}

	_, err := topo.NewStoreFromShard(context.Background(), topoTestClient(), shard)
	if err == nil {
		t.Fatal("expected an error for a missing client credential Secret")
	}
	if !strings.Contains(err.Error(), tlsName) {
		t.Errorf("error does not name the missing Secret: %v", err)
	}
}

// A Secret that exists but is missing a required key also fails loudly, naming
// the key.
func TestNewStoreFromRef_MissingKeyIsLoud(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "topo-tls", Namespace: "team-a"},
		Data: map[string][]byte{
			"tls.crt": []byte("cert"),
			// tls.key intentionally absent.
			"ca.crt": []byte("ca"),
		},
	}
	ref := multigresv1alpha1.GlobalTopoServerRef{
		Address:          "localhost:2379",
		RootPath:         "/test",
		CASecret:         "topo-tls",
		ClientCertSecret: "topo-tls",
	}

	_, err := topo.NewStoreFromRef(context.Background(), topoTestClient(secret), "team-a", ref)
	if err == nil {
		t.Fatal("expected an error for a Secret missing tls.key")
	}
	if !strings.Contains(err.Error(), "tls.key") || !strings.Contains(err.Error(), "topo-tls") {
		t.Errorf("error does not name the Secret and missing key: %v", err)
	}
}

func TestNewStoreFromRef_WithClientCert(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "topo-tls", Namespace: "team-a"},
		Data: map[string][]byte{
			"tls.crt": []byte("cert"),
			"tls.key": []byte("key"),
			"ca.crt":  []byte("ca"),
		},
	}
	ref := multigresv1alpha1.GlobalTopoServerRef{
		Address:          "localhost:2379",
		RootPath:         "/test",
		CASecret:         "topo-tls",
		ClientCertSecret: "topo-tls",
	}

	store, err := topo.NewStoreFromRef(context.Background(), topoTestClient(secret), "team-a", ref)
	if err != nil {
		t.Fatalf("NewStoreFromRef() unexpected error: %v", err)
	}
	if store != nil {
		_ = store.Close()
	}
}

func TestIsTopoUnavailable(t *testing.T) {
	tests := map[string]struct {
		err  error
		want bool
	}{
		"nil error":           {err: nil, want: false},
		"UNAVAILABLE error":   {err: errors.New("Code: UNAVAILABLE"), want: true},
		"no connection error": {err: errors.New("no connection available"), want: true},
		"unrelated error":     {err: errors.New("permission denied"), want: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := topo.IsTopoUnavailable(tc.err); got != tc.want {
				t.Errorf("IsTopoUnavailable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestNewStoreFromCell(t *testing.T) {
	cell := &multigresv1alpha1.Cell{
		Spec: multigresv1alpha1.CellSpec{
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:  "localhost:2379",
				RootPath: "/test",
			},
		},
	}

	store, err := topo.NewStoreFromCell(context.Background(), topoTestClient(), cell)
	if err != nil {
		t.Fatalf("NewStoreFromCell() unexpected error: %v", err)
	}
	if store != nil {
		_ = store.Close()
	}
}

func TestNewStoreFromCell_InvalidImplementation(t *testing.T) {
	cell := &multigresv1alpha1.Cell{
		Spec: multigresv1alpha1.CellSpec{
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "localhost:2379",
				RootPath:       "/test",
				Implementation: "invalid-implementation-that-does-not-exist",
			},
		},
	}

	store, err := topo.NewStoreFromCell(context.Background(), topoTestClient(), cell)
	if err == nil {
		if store != nil {
			_ = store.Close()
		}
		t.Error("NewStoreFromCell() should error with invalid implementation")
	}
}

func TestNewStoreFromRef(t *testing.T) {
	ref := multigresv1alpha1.GlobalTopoServerRef{
		Address:  "localhost:2379",
		RootPath: "/test",
	}

	store, err := topo.NewStoreFromRef(context.Background(), topoTestClient(), "team-a", ref)
	if err != nil {
		t.Fatalf("NewStoreFromRef() unexpected error: %v", err)
	}
	if store != nil {
		_ = store.Close()
	}
}

func TestNewStoreFromRef_InvalidImplementation(t *testing.T) {
	ref := multigresv1alpha1.GlobalTopoServerRef{
		Address:        "localhost:2379",
		RootPath:       "/test",
		Implementation: "invalid-implementation-that-does-not-exist",
	}

	store, err := topo.NewStoreFromRef(context.Background(), topoTestClient(), "team-a", ref)
	if err == nil {
		if store != nil {
			_ = store.Close()
		}
		t.Error("NewStoreFromRef() should error with invalid implementation")
	}
}
