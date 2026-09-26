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
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	c := assert.NewAborting(t)
	s := runtime.NewScheme()
	c.NoError(clientgoscheme.AddToScheme(s), "add scheme")
	c.NoError(multigresv1alpha1.AddToScheme(s), "add Multigres scheme")
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

func activeTLSShard(
	r *OperatorCertResolver,
	name,
	namespace,
	cluster string,
) *multigresv1alpha1.Shard {
	r.ActivateCluster(types.NamespacedName{Namespace: namespace, Name: cluster})
	return testShard(name, namespace, cluster, true)
}

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	c := assert.NewAborting(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	c.NoError(err, "generate CA key")
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
	c.NoError(err, "create CA")
	cert, err := x509.ParseCertificate(der)
	c.NoError(err, "parse CA")
	return &testCA{
		cert: cert,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

func (ca *testCA) issue(
	t *testing.T,
	dnsNames []string,
	usages ...x509.ExtKeyUsage,
) (certPEM, keyPEM []byte, leaf *x509.Certificate) {
	t.Helper()
	c := assert.NewAborting(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	c.NoError(err, "generate leaf key")
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
	c.NoError(err, "create leaf")
	leaf, err = x509.ParseCertificate(der)
	c.NoError(err, "parse leaf")
	keyDER, err := x509.MarshalECPrivateKey(key)
	c.NoError(err, "marshal leaf key")
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), leaf
}

func operatorSecret(
	t *testing.T,
	ca *testCA,
	namespace,
	cluster,
	resourceVersion string,
) *corev1.Secret {
	t.Helper()
	certPEM, keyPEM, _ := ca.issue(t, nil, x509.ExtKeyUsageClientAuth)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: multigresv1alpha1.ComponentCertSecretName(
				multigresv1alpha1.ComponentOperatorTLS,
				cluster,
				namespace,
			),
			Namespace:       namespace,
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
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objects...).Build()
	insecure := rpcclient.NewFakeClient()
	r, err := NewOperatorCertResolver(c, Options{Capacity: 10, Insecure: insecure})
	assert.NewAborting(t).NoError(err, "NewOperatorCertResolver() error =")
	t.Cleanup(r.Close)
	return r, insecure, c
}

func TestNewOperatorCertResolverValidatesDependencies(t *testing.T) {
	insecure := rpcclient.NewFakeClient()
	reader := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	tests := []struct {
		name   string
		reader client.Reader
		opts   Options
		want   string
	}{
		{name: "missing reader", opts: Options{Insecure: insecure}, want: "Secret reader"},
		{name: "missing insecure client", reader: reader, want: "insecure client"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewOperatorCertResolver(tt.reader, tt.opts); err == nil ||
				!strings.Contains(err.Error(), tt.want) {
				t.Fatalf("NewOperatorCertResolver() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestStaticResolver(t *testing.T) {
	c := assert.NewCollecting(t)
	want := rpcclient.NewFakeClient()
	got, err := Static(want).ClientFor(t.Context(), testShard("s", "ns", "c", true))
	c.Require().NoError(err, "ClientFor() error =")
	c.False(got != want, "Static resolver returned a different client")
}

func TestClientForTLSDisabledReturnsInsecure(t *testing.T) {
	c := assert.NewCollecting(t)
	r, insecure, _ := newResolver(t)
	got, err := r.ClientFor(t.Context(), testShard("s", "ns", "c", false))
	c.Require().NoError(err, "ClientFor() error =")
	c.False(got != insecure, "TLS-disabled shard did not get the insecure client")
}

func TestClientForRequiresClusterLabel(t *testing.T) {
	r, _, _ := newResolver(t)
	_, err := r.ClientFor(t.Context(), testShard("s", "ns", "", true))
	assert.NewAborting(t).
		False(err == nil || !strings.Contains(err.Error(), metadata.LabelMultigresCluster), "ClientFor() error = %v, want missing label error", err)
}

func TestClientForSecretNotIssued(t *testing.T) {
	r, _, _ := newResolver(t)
	_, err := r.ClientFor(t.Context(), activeTLSShard(r, "s", "ns", "c"))
	assert.NewAborting(t).
		False(err == nil || !strings.Contains(err.Error(), "reading operator internal TLS secret"), "ClientFor() error = %v, want missing Secret error", err)
}

type countingBlockingReader struct {
	client.Reader

	mu      sync.Mutex
	gets    int
	started chan struct{}
	release chan struct{}
}

func (r *countingBlockingReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	r.mu.Lock()
	r.gets++
	r.mu.Unlock()
	select {
	case r.started <- struct{}{}:
	default:
	}
	select {
	case <-r.release:
		return r.Reader.Get(ctx, key, obj, opts...)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *countingBlockingReader) getCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gets
}

func TestClientForCoalescesAndCachesInitialFailure(t *testing.T) {
	c := assert.NewAborting(t)
	r, _, reader := newResolver(t)
	blocking := &countingBlockingReader{
		Reader:  reader,
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	r.reader = blocking
	r.failureRetry = time.Hour
	shard := activeTLSShard(r, "s", "ns", "c")

	const callers = 20
	errs := make(chan error, callers)
	for range callers {
		go func() {
			_, err := r.ClientFor(t.Context(), shard)
			errs <- err
		}()
	}
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("initial Secret read did not start")
	}
	// Give the other callers a chance to join the in-flight initialization.
	time.Sleep(20 * time.Millisecond)
	c.Eq(1, blocking.getCount(), "concurrent initial Secret reads")
	close(blocking.release)
	for range callers {
		if err := <-errs; err == nil ||
			!strings.Contains(err.Error(), "reading operator internal TLS secret") {
			t.Errorf("ClientFor() error = %v, want missing Secret error", err)
		}
	}

	_, err := r.ClientFor(t.Context(), shard)
	c.Error(err, "ClientFor() expected cached missing Secret error")
	c.Eq(1, blocking.getCount(), "Secret reads during failure cache")
}

type cancelFirstReader struct {
	client.Reader

	mu      sync.Mutex
	gets    int
	started chan struct{}
}

func (r *cancelFirstReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	r.mu.Lock()
	r.gets++
	call := r.gets
	r.mu.Unlock()
	if call == 1 {
		close(r.started)
		<-ctx.Done()
		return ctx.Err()
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r *cancelFirstReader) getCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gets
}

func TestClientForCanceledColdLeaderDoesNotPoisonJoiner(t *testing.T) {
	c := assert.NewAborting(t)
	ca := newTestCA(t)
	r, _, reader := newResolver(t, operatorSecret(t, ca, "ns", "c", "1"))
	cancelReader := &cancelFirstReader{
		Reader:  reader,
		started: make(chan struct{}),
	}
	r.reader = cancelReader
	shard := activeTLSShard(r, "s", "ns", "c")
	leaderCtx, cancelLeader := context.WithCancel(t.Context())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := r.ClientFor(leaderCtx, shard)
		leaderErr <- err
	}()
	select {
	case <-cancelReader.started:
	case <-time.After(time.Second):
		t.Fatal("cold initialization did not start")
	}

	joinerResult := make(chan struct {
		client rpcclient.MultipoolerClient
		err    error
	}, 1)
	go func() {
		client, err := r.ClientFor(t.Context(), shard)
		joinerResult <- struct {
			client rpcclient.MultipoolerClient
			err    error
		}{client: client, err: err}
	}()
	cancelLeader()
	if err := <-leaderErr; err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("leader ClientFor() error = %v, want context cancellation", err)
	}
	select {
	case result := <-joinerResult:
		c.False(
			result.err != nil || result.client == nil,
			"joiner ClientFor() = (%v, %v), want a client",
			result.client,
			result.err,
		)
	case <-time.After(time.Second):
		t.Fatal("healthy joiner did not retry canceled initialization")
	}
	c.Eq(2, cancelReader.getCount(), "Secret reads")
}

func TestClientForCachesPerClusterAndBindsServerName(t *testing.T) {
	c := assert.NewCollecting(t)
	ca := newTestCA(t)
	r, insecure, _ := newResolver(t,
		operatorSecret(t, ca, "ns-a", "cluster-a", "1"),
		operatorSecret(t, ca, "ns-b", "cluster-b", "1"),
	)
	configs := make([]*tls.Config, 0, 2)
	r.newClient = func(config *tls.Config) rpcclient.MultipoolerClient {
		configs = append(configs, config)
		return rpcclient.NewFakeClient()
	}
	r.ActivateCluster(types.NamespacedName{Namespace: "ns-a", Name: "cluster-a"})
	r.ActivateCluster(types.NamespacedName{Namespace: "ns-b", Name: "cluster-b"})

	a1, err := r.ClientFor(t.Context(), testShard("s1", "ns-a", "cluster-a", true))
	c.Require().NoError(err, "first cluster A ClientFor() error =")
	a2, err := r.ClientFor(t.Context(), testShard("s2", "ns-a", "cluster-a", true))
	c.Require().NoError(err, "second cluster A ClientFor() error =")
	b, err := r.ClientFor(t.Context(), testShard("s", "ns-b", "cluster-b", true))
	c.Require().NoError(err, "cluster B ClientFor() error =")
	c.False(
		a1 == insecure || b == insecure || a1 != a2 || a1 == b,
		"clients were not cached independently per cluster",
	)
	c.Require().Len(configs, 2, "created %d TLS configs, want 2", len(configs))
	wantA := "multipooler.cluster-a.ns-a.multigres.internal"
	wantB := "multipooler.cluster-b.ns-b.multigres.internal"
	if configs[0].ServerName != wantA || configs[1].ServerName != wantB {
		t.Errorf(
			"ServerNames = %q, %q; want %q, %q",
			configs[0].ServerName,
			configs[1].ServerName,
			wantA,
			wantB,
		)
	}
	c.False(
		configs[0].InsecureSkipVerify || configs[0].VerifyConnection != nil,
		"cluster client bypasses normal TLS hostname verification",
	)
}

func TestClientForKeepsClientOnRefreshFailure(t *testing.T) {
	ck := assert.NewCollecting(t)
	ca := newTestCA(t)
	secret := operatorSecret(t, ca, "ns", "c", "1")
	r, _, c := newResolver(t, secret)
	r.refreshInterval = 0
	shard := activeTLSShard(r, "s", "ns", "c")

	want, err := r.ClientFor(t.Context(), shard)
	ck.Require().NoError(err, "first ClientFor() error =")
	ck.Require().NoError(c.Delete(t.Context(), secret), "delete secret")
	got, err := r.ClientFor(t.Context(), shard)
	ck.Require().NoError(err, "refresh ClientFor() error =")
	ck.False(got != want, "refresh failure replaced the working client")
}

type countingReader struct {
	client.Reader
	mu   sync.Mutex
	gets int
}

func (r *countingReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	r.mu.Lock()
	r.gets++
	r.mu.Unlock()
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r *countingReader) getCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gets
}

func TestClientForRetriesWarmRefreshFailureOnFailureInterval(t *testing.T) {
	ck := assert.NewAborting(t)
	ca := newTestCA(t)
	secret := operatorSecret(t, ca, "ns", "c", "1")
	r, _, c := newResolver(t, secret)
	reader := &countingReader{Reader: c}
	r.reader = reader
	r.refreshInterval = time.Hour
	r.failureRetry = time.Minute
	shard := activeTLSShard(r, "s", "ns", "c")

	want, err := r.ClientFor(t.Context(), shard)
	ck.NoError(err, "first ClientFor() error =")
	state := r.states[types.NamespacedName{Namespace: "ns", Name: "c"}]
	state.mu.Lock()
	state.fetchedAt = time.Now().Add(-2 * r.refreshInterval)
	state.mu.Unlock()
	ck.NoError(c.Delete(t.Context(), secret), "delete Secret")
	if got, err := r.ClientFor(t.Context(), shard); err != nil || got != want {
		t.Fatalf("failed refresh ClientFor() = (%v, %v), want existing client", got, err)
	}
	ck.Eq(2, reader.getCount(), "Secret reads after failed refresh")
	if _, err := r.ClientFor(t.Context(), shard); err != nil {
		t.Fatalf("ClientFor() during failure retry window error = %v", err)
	}
	ck.Eq(2, reader.getCount(), "Secret reads during failure retry window")

	replacement := operatorSecret(t, ca, "ns", "c", "")
	ck.NoError(c.Create(t.Context(), replacement), "recreate Secret")
	state.mu.Lock()
	state.fetchedAt = time.Now().Add(-2 * r.failureRetry)
	state.mu.Unlock()
	if _, err := r.ClientFor(t.Context(), shard); err != nil {
		t.Fatalf("ClientFor() after failure retry interval error = %v", err)
	}
	ck.Eq(3, reader.getCount(), "Secret reads after failure retry interval")
}

func TestClientForCanceledWarmRefreshDoesNotDelayRetry(t *testing.T) {
	ck := assert.NewAborting(t)
	ca := newTestCA(t)
	secret := operatorSecret(t, ca, "ns", "c", "1")
	r, _, c := newResolver(t, secret)
	r.refreshInterval = time.Hour
	shard := activeTLSShard(r, "s", "ns", "c")
	want, err := r.ClientFor(t.Context(), shard)
	ck.NoError(err, "first ClientFor() error =")
	state := r.states[types.NamespacedName{Namespace: "ns", Name: "c"}]
	state.mu.Lock()
	state.fetchedAt = time.Now().Add(-2 * r.refreshInterval)
	state.mu.Unlock()
	cancelReader := &cancelFirstReader{Reader: c, started: make(chan struct{})}
	r.reader = cancelReader
	refreshCtx, cancelRefresh := context.WithCancel(t.Context())
	refreshResult := make(chan struct {
		client rpcclient.MultipoolerClient
		err    error
	}, 1)
	go func() {
		client, err := r.ClientFor(refreshCtx, shard)
		refreshResult <- struct {
			client rpcclient.MultipoolerClient
			err    error
		}{client: client, err: err}
	}()
	<-cancelReader.started
	cancelRefresh()
	result := <-refreshResult
	ck.False(
		result.err != nil || result.client != want,
		"canceled refresh ClientFor() = (%v, %v), want existing client",
		result.client,
		result.err,
	)
	state.mu.Lock()
	lastErr := state.lastErr
	state.mu.Unlock()
	ck.NoError(lastErr, "canceled refresh cached error")
	if got, err := r.ClientFor(t.Context(), shard); err != nil || got != want {
		t.Fatalf("healthy retry ClientFor() = (%v, %v), want existing client", got, err)
	}
	ck.Eq(2, cancelReader.getCount(), "Secret reads")
}

type blockingReader struct {
	client.Reader
	started chan<- struct{}
	release <-chan struct{}
}

func (r *blockingReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	r.started <- struct{}{}
	select {
	case <-r.release:
		return r.Reader.Get(ctx, key, obj, opts...)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestClientForDoesNotBlockOnConcurrentRefresh(t *testing.T) {
	ck := assert.NewAborting(t)
	ca := newTestCA(t)
	secret := operatorSecret(t, ca, "ns", "c", "1")
	r, _, c := newResolver(t, secret)
	r.refreshInterval = 0
	shard := activeTLSShard(r, "s", "ns", "c")

	want, err := r.ClientFor(t.Context(), shard)
	ck.NoError(err, "first ClientFor() error =")
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	r.reader = &blockingReader{Reader: c, started: started, release: release}
	refreshDone := make(chan error, 1)
	go func() {
		_, refreshErr := r.ClientFor(t.Context(), shard)
		refreshDone <- refreshErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("background refresh did not start")
	}

	reuseDone := make(chan rpcclient.MultipoolerClient, 1)
	go func() {
		got, reuseErr := r.ClientFor(t.Context(), shard)
		if reuseErr != nil {
			reuseDone <- nil
			return
		}
		reuseDone <- got
	}()
	select {
	case got := <-reuseDone:
		if got != want {
			close(release)
			t.Fatal("concurrent reconcile did not reuse the current client")
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("concurrent reconcile blocked behind Secret refresh")
	}
	close(release)
	ck.NoError(<-refreshDone, "background refresh error =")
}

func TestClientForKeepsClientOnMalformedRotation(t *testing.T) {
	ck := assert.NewCollecting(t)
	ca := newTestCA(t)
	secret := operatorSecret(t, ca, "ns", "c", "1")
	r, _, c := newResolver(t, secret)
	r.refreshInterval = 0
	shard := activeTLSShard(r, "s", "ns", "c")

	want, err := r.ClientFor(t.Context(), shard)
	ck.Require().NoError(err, "first ClientFor() error =")
	current := &corev1.Secret{}
	key := types.NamespacedName{Namespace: secret.Namespace, Name: secret.Name}
	ck.Require().NoError(c.Get(t.Context(), key, current), "get secret")
	current.Data[corev1.TLSCertKey] = []byte("not a cert")
	ck.Require().NoError(c.Update(t.Context(), current), "update secret")
	got, err := r.ClientFor(t.Context(), shard)
	ck.Require().NoError(err, "refresh ClientFor() error =")
	ck.False(got != want, "malformed rotation replaced the working client")
}

func TestClientForRebuildsOnSecretRotation(t *testing.T) {
	ck := assert.NewCollecting(t)
	ca := newTestCA(t)
	secret := operatorSecret(t, ca, "ns", "c", "1")
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
	shard := activeTLSShard(r, "s", "ns", "c")
	first, err := r.ClientFor(t.Context(), shard)
	ck.Require().NoError(err, "first ClientFor() error =")

	current := &corev1.Secret{}
	key := types.NamespacedName{Namespace: secret.Namespace, Name: secret.Name}
	ck.Require().NoError(c.Get(t.Context(), key, current), "get secret")
	certPEM, keyPEM, _ := ca.issue(t, nil, x509.ExtKeyUsageClientAuth)
	current.Data[corev1.TLSCertKey] = certPEM
	current.Data[corev1.TLSPrivateKeyKey] = keyPEM
	ck.Require().NoError(c.Update(t.Context(), current), "rotate secret")

	after, err := r.ClientFor(t.Context(), shard)
	ck.Require().NoError(err, "post-rotation ClientFor() error =")
	ck.False(after == first, "rotated secret did not produce a new client")
	state := r.states[types.NamespacedName{Namespace: "ns", Name: "c"}]
	ck.False(
		len(state.retiredClients) != 1 || state.retiredClients[0].client != first,
		"old client was not retained for in-flight RPCs",
	)
	r.Close()
	ck.Eq(2, closeCount, "client close count after resolver shutdown")
}

type closeTrackingClient struct {
	rpcclient.MultipoolerClient
	closeCount *int
}

func (c *closeTrackingClient) Close() {
	*c.closeCount++
}

type closeSignalClient struct {
	rpcclient.MultipoolerClient
	once   sync.Once
	closed chan struct{}
}

func (c *closeSignalClient) Close() {
	c.once.Do(func() { close(c.closed) })
}

func TestForgetClusterBlocksStaleShardUntilReactivated(t *testing.T) {
	c := assert.NewAborting(t)
	ca := newTestCA(t)
	r, _, _ := newResolver(t, operatorSecret(t, ca, "ns", "c", "1"))
	r.retirementGrace = 10 * time.Millisecond
	clients := make([]*closeSignalClient, 0, 2)
	r.newClient = func(*tls.Config) rpcclient.MultipoolerClient {
		client := &closeSignalClient{
			MultipoolerClient: rpcclient.NewFakeClient(),
			closed:            make(chan struct{}),
		}
		clients = append(clients, client)
		return client
	}
	key := types.NamespacedName{Namespace: "ns", Name: "c"}
	shard := activeTLSShard(r, "s", key.Namespace, key.Name)
	first, err := r.ClientFor(t.Context(), shard)
	c.NoError(err, "first ClientFor() error =")

	r.ForgetCluster(key)
	if _, err := r.ClientFor(t.Context(), shard); err == nil ||
		!strings.Contains(err.Error(), "not found") {
		t.Fatalf("stale shard ClientFor() error = %v, want missing cluster", err)
	}
	r.mu.Lock()
	_, stateRecreated := r.states[key]
	_, stillActive := r.active[key]
	r.mu.Unlock()
	c.False(
		stateRecreated || stillActive,
		"forgotten cluster state/active = %v/%v, want false/false",
		stateRecreated,
		stillActive,
	)
	select {
	case <-clients[0].closed:
	case <-time.After(time.Second):
		t.Fatal("forgotten cluster client was not closed after the grace period")
	}

	r.ActivateCluster(key)
	after, err := r.ClientFor(t.Context(), shard)
	c.NoError(err, "reactivated ClientFor() error =")
	c.False(after == first, "reactivated cluster reused the forgotten client")
}

func TestClientForValidatesLiveClusterBeforeClusterControllerReconciles(t *testing.T) {
	c := assert.NewAborting(t)
	ca := newTestCA(t)
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
	}
	r, insecure, _ := newResolver(t, cluster, operatorSecret(t, ca, "ns", "c", "1"))

	got, err := r.ClientFor(t.Context(), testShard("s", "ns", "c", true))
	c.NoError(err, "ClientFor() before cluster reconcile error =")
	c.False(
		got == nil || got == insecure,
		"ClientFor() before cluster reconcile did not build an mTLS client",
	)
	r.mu.Lock()
	_, active := r.active[types.NamespacedName{Namespace: "ns", Name: "c"}]
	r.mu.Unlock()
	c.True(active, "live cluster was not added to lifecycle registry")
}

func TestClientForMalformedInitialSecret(t *testing.T) {
	secret := operatorSecret(t, newTestCA(t), "ns", "c", "1")
	delete(secret.Data, corev1.TLSPrivateKeyKey)
	r, _, _ := newResolver(t, secret)
	_, err := r.ClientFor(t.Context(), activeTLSShard(r, "s", "ns", "c"))
	assert.NewAborting(t).Error(err, "ClientFor() expected malformed Secret error")
}

func TestBuildTLSConfigVerifiesExactClusterIdentity(t *testing.T) {
	ca := newTestCA(t)
	secret := operatorSecret(t, ca, "ns-a", "cluster-a", "1")
	serverName := "multipooler.cluster-a.ns-a.multigres.internal"
	config, err := buildTLSConfig(secret, serverName)
	assert.NewAborting(t).NoError(err, "buildTLSConfig() error =")
	if config.ServerName != serverName || config.InsecureSkipVerify {
		t.Fatalf(
			"TLS config ServerName/InsecureSkipVerify = %q/%v",
			config.ServerName,
			config.InsecureSkipVerify,
		)
	}

	foreignCA := newTestCA(t)
	tests := []struct {
		name     string
		issuer   *testCA
		dnsNames []string
		usage    x509.ExtKeyUsage
		wantOK   bool
	}{
		{
			name:     "target multipooler",
			issuer:   ca,
			dnsNames: []string{serverName},
			usage:    x509.ExtKeyUsageServerAuth,
			wantOK:   true,
		},
		{
			name:     "other cluster",
			issuer:   ca,
			dnsNames: []string{"multipooler.cluster-b.ns-a.multigres.internal"},
			usage:    x509.ExtKeyUsageServerAuth,
		},
		{
			// Multigateway deliberately carries the same-cluster multipooler
			// alias, so standard hostname verification accepts that shared
			// serving certificate while still rejecting another cluster.
			name:   "same cluster multigateway certificate with pooler alias",
			issuer: ca,
			dnsNames: []string{
				"multigateway.cluster-a.ns-a.multigres.internal",
				serverName,
			},
			usage:  x509.ExtKeyUsageServerAuth,
			wantOK: true,
		},
		{
			name:     "foreign issuer",
			issuer:   foreignCA,
			dnsNames: []string{serverName},
			usage:    x509.ExtKeyUsageServerAuth,
		},
		{
			name:     "client auth only",
			issuer:   ca,
			dnsNames: []string{serverName},
			usage:    x509.ExtKeyUsageClientAuth,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, leaf := tt.issuer.issue(t, tt.dnsNames, tt.usage)
			_, err := leaf.Verify(x509.VerifyOptions{
				Roots:   config.RootCAs,
				DNSName: config.ServerName,
			})
			assert.NewCollecting(t).
				Eq(tt.wantOK, (err == nil), "Verify() error = %v, want success", err)
		})
	}
}
