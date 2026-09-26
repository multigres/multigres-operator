package toposerver

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/multigres/testkit/assert"
)

func TestMaintenanceTLSUsesUncachedCredentialAndFailsClosed(t *testing.T) {
	c := assert.NewAborting(t)
	ts := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	c.NoError(err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	c.NoError(err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	c.NoError(err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      multigresv1alpha1.TopoServerCertSecretName(ts.Name),
			Namespace: ts.Namespace,
		},
		Data: map[string][]byte{
			"tls.crt": certPEM,
			"tls.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
			"ca.crt":  certPEM,
		},
	}
	for _, test := range []struct {
		name    string
		change  func(*corev1.Secret)
		wantErr bool
	}{
		{"valid", func(*corev1.Secret) {}, false},
		{"missing key", func(s *corev1.Secret) { delete(s.Data, "tls.key") }, true},
		{"invalid CA", func(s *corev1.Secret) { s.Data["ca.crt"] = []byte("invalid") }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := assert.NewAborting(t)
			s := secret.DeepCopy()
			test.change(s)
			r := &TopoServerReconciler{
				Client:    fake.NewClientBuilder().WithScheme(certScheme()).Build(),
				APIReader: fake.NewClientBuilder().WithScheme(certScheme()).WithObjects(s).Build(),
			}
			cfg, err := r.maintenanceTLSConfig(t.Context(), ts)
			c.ErrorWhen(test.wantErr, err, "error=")
			c.False(!test.wantErr &&
				(cfg.MinVersion < tls.VersionTLS12 || cfg.InsecureSkipVerify || cfg.RootCAs == nil || len(cfg.Certificates) != 1), "invalid TLS configuration")
		})
	}
}
