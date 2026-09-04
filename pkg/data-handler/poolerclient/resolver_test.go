package poolerclient

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const operatorNamespace = "multigres-operator"

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	if err := multigresv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add Multigres scheme: %v", err)
	}
	return s
}

func testShard(name, namespace, cluster string, tlsEnabled bool) *multigresv1alpha1.Shard {
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{metadata.LabelMultigresCluster: cluster},
		},
	}
	if tlsEnabled {
		enabled := true
		shard.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: &enabled}
	}
	return shard
}

func testCluster(namespace, name, issuerName string) *multigresv1alpha1.MultigresCluster {
	return &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       multigresv1alpha1.MultigresClusterSpec{IssuerName: issuerName},
	}
}

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	return &testCA{
		cert: cert,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// issue signs a leaf with the given SANs and ext key usages; returns cert PEM,
// key PEM, and the parsed certificate.
func (ca *testCA) issue(
	t *testing.T,
	dnsNames []string,
	usages ...x509.ExtKeyUsage,
) (certPEM, keyPEM []byte, leaf *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
		DNSNames:     dnsNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}
	leaf, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, leaf
}

func operatorSecret(t *testing.T, ca *testCA, resourceVersion string) *corev1.Secret {
	t.Helper()
	certPEM, keyPEM, _ := ca.issue(t, nil, x509.ExtKeyUsageClientAuth)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            CertificateName,
			Namespace:       operatorNamespace,
			ResourceVersion: resourceVersion,
		},
		Data: map[string][]byte{
			corev1.TLSCertKey:              certPEM,
			corev1.TLSPrivateKeyKey:        keyPEM,
			corev1.ServiceAccountRootCAKey: ca.pem,
		},
	}
}

func newResolver(
	t *testing.T,
	objects ...client.Object,
) (*OperatorCertResolver, rpcclient.MultipoolerClient, client.Client) {
	t.Helper()
	objects = append(objects,
		testCluster("ns", "c", ""),
		testCluster("ns-a", "mgc-a", ""),
		testCluster("ns-b", "mgc-b", ""),
	)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objects...).Build()
	insecure := rpcclient.NewFakeClient()
	r := NewOperatorCertResolver(c, c, Options{
		Namespace: operatorNamespace,
		Capacity:  10,
		Insecure:  insecure,
	})
	// The fake client has no cert-manager CRD; treat issuance as done.
	r.applyCert = func(context.Context, string) error { return nil }
	t.Cleanup(r.Close)
	return r, insecure, c
}

func TestStaticResolver(t *testing.T) {
	want := rpcclient.NewFakeClient()
	got, err := Static(want).ClientFor(t.Context(), testShard("s", "ns", "c", true))
	if err != nil {
		t.Fatalf("ClientFor() error = %v", err)
	}
	if got != want {
		t.Error("Static resolver returned a different client")
	}
}

func TestClientForTLSDisabledReturnsInsecure(t *testing.T) {
	r, insecure, _ := newResolver(t)
	got, err := r.ClientFor(t.Context(), testShard("s", "ns", "c", false))
	if err != nil {
		t.Fatalf("ClientFor() error = %v", err)
	}
	if got != insecure {
		t.Error("TLS-disabled shard did not get the insecure client")
	}
}

func TestClientForSecretNotIssued(t *testing.T) {
	r, _, _ := newResolver(t)
	_, err := r.ClientFor(t.Context(), testShard("s", "ns", "c", true))
	if err == nil {
		t.Fatal("ClientFor() expected error for missing secret")
	}
	if !strings.Contains(err.Error(), "not issued") {
		t.Errorf("error %q does not say the secret is not issued", err)
	}
	if state := r.states[r.opts.IssuerName]; state == nil || state.certApplied {
		t.Error("missing secret did not schedule the Certificate to be re-applied")
	}
}

func TestClientForApplyCertificateFailure(t *testing.T) {
	r, _, _ := newResolver(t)
	want := errors.New("boom")
	r.applyCert = func(context.Context, string) error { return want }
	if _, err := r.ClientFor(t.Context(), testShard("s", "ns", "c", true)); !errors.Is(err, want) {
		t.Fatalf("ClientFor() error = %v, want %v", err, want)
	}
	if state := r.states[r.opts.IssuerName]; state == nil || state.certApplied {
		t.Error("failed apply was recorded as applied")
	}
}

func TestClientForSharesOneClientAcrossClusters(t *testing.T) {
	ca := newTestCA(t)
	r, insecure, _ := newResolver(t, operatorSecret(t, ca, "1"))

	first, err := r.ClientFor(t.Context(), testShard("s1", "ns-a", "mgc-a", true))
	if err != nil {
		t.Fatalf("first ClientFor() error = %v", err)
	}
	if first == nil || first == insecure {
		t.Fatal("TLS-enabled shard did not get a dedicated TLS client")
	}

	other, err := r.ClientFor(t.Context(), testShard("s2", "ns-b", "mgc-b", true))
	if err != nil {
		t.Fatalf("second ClientFor() error = %v", err)
	}
	if other != first {
		t.Error("shard in a different cluster did not share the single TLS client")
	}
}

func TestClientForUsesOneClientPerIssuer(t *testing.T) {
	defaultCA := newTestCA(t)
	customCA := newTestCA(t)
	customIssuer := "tenant-issuer"
	customSecret := operatorSecret(t, customCA, "1")
	customSecret.Name = certificateNameForIssuer(customIssuer, "supabase-issuer")
	r, insecure, c := newResolver(t,
		operatorSecret(t, defaultCA, "1"),
		customSecret,
	)
	customCluster := &multigresv1alpha1.MultigresCluster{}
	if err := c.Get(
		t.Context(),
		client.ObjectKey{Namespace: "ns-b", Name: "mgc-b"},
		customCluster,
	); err != nil {
		t.Fatalf("get custom cluster: %v", err)
	}
	customCluster.Spec.IssuerName = customIssuer
	if err := c.Update(t.Context(), customCluster); err != nil {
		t.Fatalf("update custom cluster: %v", err)
	}

	legacyShard := testShard("legacy", "ns-a", "mgc-a", true)
	legacy, err := r.ClientFor(t.Context(), legacyShard)
	if err != nil {
		t.Fatalf("legacy ClientFor() error = %v", err)
	}
	customShard := testShard("custom", "ns-b", "mgc-b", true)
	custom, err := r.ClientFor(t.Context(), customShard)
	if err != nil {
		t.Fatalf("custom ClientFor() error = %v", err)
	}
	if legacy == insecure || custom == insecure || legacy == custom {
		t.Error("distinct issuers did not receive distinct TLS clients")
	}
	if len(r.states) != 2 {
		t.Errorf("issuer state count = %d, want 2", len(r.states))
	}
}

func TestClientForRequiresOwningCluster(t *testing.T) {
	r, _, _ := newResolver(t)
	tests := []struct {
		name  string
		shard *multigresv1alpha1.Shard
		want  string
	}{
		{
			name:  "missing cluster label",
			shard: testShard("s", "ns", "", true),
			want:  metadata.LabelMultigresCluster,
		},
		{
			name:  "missing cluster",
			shard: testShard("s", "ns", "missing", true),
			want:  "reading owning MultigresCluster",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := r.ClientFor(t.Context(), tt.shard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ClientFor() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestClientForPeriodicallyReappliesCertificate(t *testing.T) {
	ca := newTestCA(t)
	r, _, _ := newResolver(t, operatorSecret(t, ca, "1"))
	r.refreshInterval = time.Hour
	applyCount := 0
	r.applyCert = func(context.Context, string) error {
		applyCount++
		return nil
	}
	shard := testShard("s", "ns", "c", true)

	if _, err := r.ClientFor(t.Context(), shard); err != nil {
		t.Fatalf("first ClientFor() error = %v", err)
	}
	if _, err := r.ClientFor(t.Context(), shard); err != nil {
		t.Fatalf("cached ClientFor() error = %v", err)
	}
	if applyCount != 1 {
		t.Fatalf("apply count inside refresh interval = %d, want 1", applyCount)
	}

	state := r.states[r.opts.IssuerName]
	state.mu.Lock()
	state.certAppliedAt = time.Now().Add(-2 * r.refreshInterval)
	state.mu.Unlock()
	if _, err := r.ClientFor(t.Context(), shard); err != nil {
		t.Fatalf("periodic ClientFor() error = %v", err)
	}
	if applyCount != 2 {
		t.Errorf("apply count after refresh interval = %d, want 2", applyCount)
	}
}

func TestClientForKeepsClientWhenPeriodicCertificateApplyFails(t *testing.T) {
	ca := newTestCA(t)
	r, _, _ := newResolver(t, operatorSecret(t, ca, "1"))
	r.refreshInterval = time.Hour
	shard := testShard("s", "ns", "c", true)

	want, err := r.ClientFor(t.Context(), shard)
	if err != nil {
		t.Fatalf("first ClientFor() error = %v", err)
	}
	state := r.states[r.opts.IssuerName]
	state.mu.Lock()
	state.certAppliedAt = time.Now().Add(-2 * r.refreshInterval)
	state.mu.Unlock()
	applyCount := 0
	r.applyCert = func(context.Context, string) error {
		applyCount++
		return errors.New("transient API failure")
	}

	got, err := r.ClientFor(t.Context(), shard)
	if err != nil {
		t.Fatalf("periodic ClientFor() error = %v", err)
	}
	if got != want {
		t.Error("periodic apply failure replaced the working TLS client")
	}
	if _, err := r.ClientFor(t.Context(), shard); err != nil {
		t.Fatalf("throttled ClientFor() error = %v", err)
	}
	if applyCount != 1 {
		t.Errorf("periodic apply attempts inside refresh interval = %d, want 1", applyCount)
	}
}

func TestClientForRebuildsOnSecretRotation(t *testing.T) {
	ca := newTestCA(t)
	secret := operatorSecret(t, ca, "1")
	r, _, c := newResolver(t, secret)
	r.refreshInterval = 0
	r.retirementGrace = time.Hour
	closeCount := 0
	r.newClient = func(*tls.Config) rpcclient.MultipoolerClient {
		return &closeTrackingClient{
			MultipoolerClient: rpcclient.NewFakeClient(),
			closeCount:        &closeCount,
		}
	}

	first, err := r.ClientFor(t.Context(), testShard("s", "ns", "c", true))
	if err != nil {
		t.Fatalf("first ClientFor() error = %v", err)
	}

	unchanged, err := r.ClientFor(t.Context(), testShard("s", "ns", "c", true))
	if err != nil {
		t.Fatalf("unchanged ClientFor() error = %v", err)
	}
	if unchanged != first {
		t.Error("refresh with same ResourceVersion replaced the client")
	}

	rotated := operatorSecret(t, ca, "")
	rotated.ResourceVersion = secret.ResourceVersion
	if err := c.Update(t.Context(), rotated); err != nil {
		t.Fatalf("update secret: %v", err)
	}

	after, err := r.ClientFor(t.Context(), testShard("s", "ns", "c", true))
	if err != nil {
		t.Fatalf("post-rotation ClientFor() error = %v", err)
	}
	if after == first {
		t.Error("rotated secret did not produce a new client")
	}
	state := r.states[r.opts.IssuerName]
	if len(state.retiredClients) != 1 || state.retiredClients[0].client != first {
		t.Error("rotated client was not retained until resolver shutdown")
	}
	if closeCount != 0 {
		t.Errorf("client close count before resolver shutdown = %d, want 0", closeCount)
	}
	state.mu.Lock()
	state.retiredClients[0].timer.Reset(time.Millisecond)
	state.mu.Unlock()
	deadline := time.Now().Add(time.Second)
	for {
		state.mu.Lock()
		retiredCount := len(state.retiredClients)
		gotCloseCount := closeCount
		state.mu.Unlock()
		if retiredCount == 0 && gotCloseCount == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"retired clients = %d, close count = %d; want 0/1 after grace",
				retiredCount,
				gotCloseCount,
			)
		}
		time.Sleep(time.Millisecond)
	}
	r.Close()
	if closeCount != 2 {
		t.Errorf("client close count after resolver shutdown = %d, want 2", closeCount)
	}
}

type closeTrackingClient struct {
	rpcclient.MultipoolerClient
	closeCount *int
}

func (c *closeTrackingClient) Close() {
	*c.closeCount++
}

func TestClientForMalformedSecret(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.Secret)
	}{
		{
			name:   "garbage cert",
			mutate: func(s *corev1.Secret) { s.Data[corev1.TLSCertKey] = []byte("not a cert") },
		},
		{
			name:   "missing key",
			mutate: func(s *corev1.Secret) { delete(s.Data, corev1.TLSPrivateKeyKey) },
		},
		{
			name:   "garbage ca",
			mutate: func(s *corev1.Secret) { s.Data[corev1.ServiceAccountRootCAKey] = []byte("nope") },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret := operatorSecret(t, newTestCA(t), "1")
			tt.mutate(secret)
			r, _, _ := newResolver(t, secret)
			if _, err := r.ClientFor(t.Context(), testShard("s", "ns", "c", true)); err == nil {
				t.Fatal("ClientFor() expected error")
			}
		})
	}
}

func TestBuildCertificateSpec(t *testing.T) {
	certificateName := certificateNameForIssuer("custom-issuer", "default-issuer")
	cert := buildCertificate(operatorNamespace, "custom-issuer", certificateName)
	if cert.GetName() != certificateName || cert.GetNamespace() != operatorNamespace {
		t.Errorf("name/namespace = %s/%s", cert.GetNamespace(), cert.GetName())
	}
	spec := cert.Object["spec"].(map[string]any)
	if spec["secretName"] != certificateName {
		t.Errorf("secretName = %v", spec["secretName"])
	}
	if issuer := spec["issuerRef"].(map[string]any)["name"]; issuer != "custom-issuer" {
		t.Errorf("issuer = %v", issuer)
	}
	if !strings.Contains(spec["literalSubject"].(string),
		"CN=multigres-operator."+operatorNamespace+".multigres.internal") {
		t.Errorf("literalSubject = %v", spec["literalSubject"])
	}
	if len(cert.GetOwnerReferences()) != 0 {
		t.Error("operator certificate must not carry an owner reference")
	}
}

func TestCertificateNameForIssuer(t *testing.T) {
	if got := certificateNameForIssuer("default", "default"); got != CertificateName {
		t.Errorf("default issuer certificate name = %q, want %q", got, CertificateName)
	}
	first := certificateNameForIssuer("Issuer.With/Characters", "default")
	second := certificateNameForIssuer("Issuer.With/Characters", "default")
	other := certificateNameForIssuer("other", "default")
	if first != second || first == other {
		t.Errorf(
			"certificate names are not stable and issuer-specific: %q %q %q",
			first,
			second,
			other,
		)
	}
	if len(first) > 63 || strings.ContainsAny(first, "/ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		t.Errorf("certificate name %q is not a valid short DNS label", first)
	}
}

func TestVerifyPoolerConnection(t *testing.T) {
	ca := newTestCA(t)
	otherCA := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	verify := verifyPoolerConnection(roots)

	_, _, pooler := ca.issue(t,
		[]string{"multipooler.mgc-a.ns-a.multigres.internal"}, x509.ExtKeyUsageServerAuth)
	_, _, admin := ca.issue(t,
		[]string{"multiadmin.mgc-a.ns-a.multigres.internal"}, x509.ExtKeyUsageServerAuth)
	_, _, foreign := otherCA.issue(t,
		[]string{"multipooler.mgc-a.ns-a.multigres.internal"}, x509.ExtKeyUsageServerAuth)
	_, _, clientOnly := ca.issue(t,
		[]string{"multipooler.mgc-a.ns-a.multigres.internal"}, x509.ExtKeyUsageClientAuth)

	tests := []struct {
		name    string
		peers   []*x509.Certificate
		wantErr bool
	}{
		{"pooler from trusted CA", []*x509.Certificate{pooler}, false},
		{"no certificate", nil, true},
		{"non-pooler SAN", []*x509.Certificate{admin}, true},
		{"untrusted CA", []*x509.Certificate{foreign}, true},
		{"missing server auth usage", []*x509.Certificate{clientOnly}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verify(tls.ConnectionState{PeerCertificates: tt.peers})
			if (err != nil) != tt.wantErr {
				t.Fatalf("verify() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
