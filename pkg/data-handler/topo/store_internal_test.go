package topo

import (
	"bytes"
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func tlsTestClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func TestLoadClientTLS_PlaintextWhenNoSecrets(t *testing.T) {
	ref := multigresv1alpha1.GlobalTopoServerRef{Address: "localhost:2379"}
	opts, err := loadClientTLS(context.Background(), tlsTestClient(), "team-a", ref)
	if err != nil {
		t.Fatalf("loadClientTLS() error = %v", err)
	}
	if opts != nil {
		t.Errorf("expected nil TLS options for a plaintext reference, got %+v", opts)
	}
}

// The managed path names one Secret for both the keypair and the CA. The loaded
// options have to carry the tls.crt, tls.key, and ca.crt bytes verbatim.
func TestLoadClientTLS_ManagedSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "topo-tls", Namespace: "team-a"},
		Data: map[string][]byte{
			"tls.crt": []byte("CERT"),
			"tls.key": []byte("KEY"),
			"ca.crt":  []byte("CA"),
		},
	}
	ref := multigresv1alpha1.GlobalTopoServerRef{
		Address:          "localhost:2379",
		CASecret:         "topo-tls",
		ClientCertSecret: "topo-tls",
	}

	opts, err := loadClientTLS(context.Background(), tlsTestClient(secret), "team-a", ref)
	if err != nil {
		t.Fatalf("loadClientTLS() error = %v", err)
	}
	if opts == nil {
		t.Fatal("expected TLS options, got nil")
	}
	if !bytes.Equal(opts.CertPEM, []byte("CERT")) ||
		!bytes.Equal(opts.KeyPEM, []byte("KEY")) ||
		!bytes.Equal(opts.CAPEM, []byte("CA")) {
		t.Errorf("TLS options do not carry the Secret material verbatim: %+v", opts)
	}
}

// An external topology can split the CA and the client keypair across two
// Secrets; both are read.
func TestLoadClientTLS_SeparateCAAndClientSecrets(t *testing.T) {
	client := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "client", Namespace: "team-a"},
		Data:       map[string][]byte{"tls.crt": []byte("CERT"), "tls.key": []byte("KEY")},
	}
	ca := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ca", Namespace: "team-a"},
		Data:       map[string][]byte{"ca.crt": []byte("CA")},
	}
	ref := multigresv1alpha1.GlobalTopoServerRef{
		Address:          "localhost:2379",
		CASecret:         "ca",
		ClientCertSecret: "client",
	}

	opts, err := loadClientTLS(context.Background(), tlsTestClient(client, ca), "team-a", ref)
	if err != nil {
		t.Fatalf("loadClientTLS() error = %v", err)
	}
	if !bytes.Equal(opts.CertPEM, []byte("CERT")) || !bytes.Equal(opts.CAPEM, []byte("CA")) {
		t.Errorf("expected material from both Secrets: %+v", opts)
	}
}
