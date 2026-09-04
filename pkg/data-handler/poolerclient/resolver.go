// Package poolerclient resolves the multipooler RPC client a shard must use:
// a shared operator mTLS identity per issuer for InternalTLS clusters, insecure
// otherwise.
package poolerclient

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/certs"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	// CertificateName is the cert-manager Certificate (and Secret) holding the
	// operator's internal client identity.
	CertificateName = "multigres-operator-internal-tls"

	defaultRefreshInterval = 5 * time.Minute
	// The shard controller bounds data-plane work, and therefore every use of a
	// resolved client, to 30 seconds. Keep the previous generation alive for
	// twice that deadline so rotation cannot interrupt an in-flight RPC.
	defaultRetirementGracePeriod = time.Minute
)

// Resolver returns the multipooler RPC client appropriate for a shard.
type Resolver interface {
	ClientFor(
		ctx context.Context,
		shard *multigresv1alpha1.Shard,
	) (rpcclient.MultipoolerClient, error)
}

type staticResolver struct {
	client rpcclient.MultipoolerClient
}

func (s staticResolver) ClientFor(
	context.Context,
	*multigresv1alpha1.Shard,
) (rpcclient.MultipoolerClient, error) {
	return s.client, nil
}

// Static returns a Resolver that always yields c.
func Static(c rpcclient.MultipoolerClient) Resolver {
	return staticResolver{client: c}
}

// Options configures OperatorCertResolver.
type Options struct {
	// Namespace is the operator's own namespace, where the Certificate lives.
	Namespace string
	// IssuerName is used when the owning cluster does not configure an issuer.
	IssuerName string
	Capacity   int
	Insecure   rpcclient.MultipoolerClient
}

// OperatorCertResolver issues one cert-manager client Certificate per issuer
// and shares one mTLS MultipoolerClient across InternalTLS clusters using that
// issuer. Poolers verify clients by chain to their issuer CA only.
type OperatorCertResolver struct {
	client client.Client
	reader client.Reader
	opts   Options

	refreshInterval time.Duration
	retirementGrace time.Duration
	applyCert       func(ctx context.Context, issuerName string) error
	newClient       func(*tls.Config) rpcclient.MultipoolerClient

	mu     sync.Mutex
	states map[string]*issuerState
	closed bool
}

type issuerState struct {
	mu              sync.Mutex
	closed          bool
	certApplied     bool
	certAppliedAt   time.Time
	tlsClient       rpcclient.MultipoolerClient
	retiredClients  []*retiredClient
	resourceVersion string
	fetchedAt       time.Time
}

type retiredClient struct {
	client rpcclient.MultipoolerClient
	timer  *time.Timer
}

// NewOperatorCertResolver creates a resolver. reader must be uncached:
// cert-manager Secrets lack the operator's managed-by label and are invisible
// to the filtered informer cache.
func NewOperatorCertResolver(
	c client.Client,
	reader client.Reader,
	opts Options,
) *OperatorCertResolver {
	if opts.IssuerName == "" {
		opts.IssuerName = certs.DefaultIssuerName
	}
	r := &OperatorCertResolver{
		client:          c,
		reader:          reader,
		opts:            opts,
		refreshInterval: defaultRefreshInterval,
		retirementGrace: defaultRetirementGracePeriod,
		states:          make(map[string]*issuerState),
	}
	r.applyCert = r.applyCertificate
	r.newClient = func(tlsConfig *tls.Config) rpcclient.MultipoolerClient {
		return rpcclient.NewMultipoolerClient(
			r.opts.Capacity,
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
	}
	return r
}

// ClientFor implements Resolver.
func (r *OperatorCertResolver) ClientFor(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (rpcclient.MultipoolerClient, error) {
	if !shard.Spec.InternalTLS.IsEnabled() {
		return r.opts.Insecure, nil
	}

	issuerName, err := r.issuerForShard(ctx, shard)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, fmt.Errorf("operator internal TLS resolver is closed")
	}
	state := r.states[issuerName]
	if state == nil {
		state = &issuerState{}
		r.states[issuerName] = state
	}
	r.mu.Unlock()

	// Issuers refresh independently: a slow cert-manager or Secret API call for
	// one issuer must not serialize every shard managed by the operator.
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return nil, fmt.Errorf("operator internal TLS resolver is closed")
	}

	now := time.Now()
	if !state.certApplied || now.Sub(state.certAppliedAt) >= r.refreshInterval {
		if err := r.applyCert(ctx, issuerName); err != nil {
			if state.tlsClient != nil {
				// A periodic repair is best-effort. Keep a working client during
				// transient API failures and throttle both the Certificate and
				// Secret checks until the next refresh window.
				state.certAppliedAt = now
				state.fetchedAt = now
				log.FromContext(ctx).Error(
					err,
					"Unable to reconcile operator internal TLS Certificate; keeping current client",
					"issuer", issuerName,
				)
				return state.tlsClient, nil
			}
			return nil, err
		}
		state.certApplied = true
		state.certAppliedAt = now
	}

	if state.tlsClient != nil && now.Sub(state.fetchedAt) < r.refreshInterval {
		return state.tlsClient, nil
	}

	secret := &corev1.Secret{}
	key := types.NamespacedName{
		Namespace: r.opts.Namespace,
		Name:      certificateNameForIssuer(issuerName, r.opts.IssuerName),
	}
	if err := r.reader.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			// Re-apply the Certificate next time in case it was deleted rather
			// than merely not issued yet.
			state.certApplied = false
			return nil, fmt.Errorf(
				"operator internal TLS secret %s not issued yet", key,
			)
		}
		return nil, fmt.Errorf("reading operator internal TLS secret %s: %w", key, err)
	}

	if state.tlsClient != nil && secret.ResourceVersion == state.resourceVersion {
		state.fetchedAt = time.Now()
		return state.tlsClient, nil
	}

	tlsConfig, err := buildTLSConfig(secret)
	if err != nil {
		return nil, fmt.Errorf("building TLS config from secret %s: %w", key, err)
	}

	if state.tlsClient != nil {
		// Existing reconciles may still be using the previous generation. Keep
		// it alive beyond the controller's data-plane deadline instead of
		// interrupting in-flight RPCs.
		r.retireClient(state, state.tlsClient)
	}
	state.tlsClient = r.newClient(tlsConfig)
	state.resourceVersion = secret.ResourceVersion
	state.fetchedAt = time.Now()
	return state.tlsClient, nil
}

func (r *OperatorCertResolver) issuerForShard(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (string, error) {
	clusterName := shard.Labels[metadata.LabelMultigresCluster]
	if clusterName == "" {
		return "", fmt.Errorf(
			"internal TLS shard %s/%s has no %s label",
			shard.Namespace,
			shard.Name,
			metadata.LabelMultigresCluster,
		)
	}

	cluster := &multigresv1alpha1.MultigresCluster{}
	key := types.NamespacedName{Namespace: shard.Namespace, Name: clusterName}
	if err := r.client.Get(ctx, key, cluster); err != nil {
		return "", fmt.Errorf("reading owning MultigresCluster %s: %w", key, err)
	}
	if cluster.Spec.IssuerName != "" {
		return cluster.Spec.IssuerName, nil
	}
	return r.opts.IssuerName, nil
}

// Close releases all current and rotated mTLS clients. The insecure client is
// owned by the caller.
func (r *OperatorCertResolver) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	states := make([]*issuerState, 0, len(r.states))
	for _, state := range r.states {
		states = append(states, state)
	}
	r.mu.Unlock()

	for _, state := range states {
		state.mu.Lock()
		state.closed = true
		if state.tlsClient != nil {
			state.tlsClient.Close()
			state.tlsClient = nil
		}
		for _, retired := range state.retiredClients {
			retired.timer.Stop()
			retired.client.Close()
		}
		state.retiredClients = nil
		state.mu.Unlock()
	}
}

// retireClient is called with state.mu held.
func (r *OperatorCertResolver) retireClient(
	state *issuerState,
	client rpcclient.MultipoolerClient,
) {
	retired := &retiredClient{client: client}
	retired.timer = time.AfterFunc(r.retirementGrace, func() {
		state.mu.Lock()
		defer state.mu.Unlock()
		for i, candidate := range state.retiredClients {
			if candidate != retired {
				continue
			}
			candidate.client.Close()
			state.retiredClients = append(
				state.retiredClients[:i],
				state.retiredClients[i+1:]...,
			)
			return
		}
	})
	state.retiredClients = append(state.retiredClients, retired)
}

func (r *OperatorCertResolver) applyCertificate(ctx context.Context, issuerName string) error {
	cert := buildCertificate(
		r.opts.Namespace,
		issuerName,
		certificateNameForIssuer(issuerName, r.opts.IssuerName),
	)
	if err := certs.ApplyOne(ctx, r.client, cert); err != nil {
		if certs.IsNoMatchError(err) {
			return fmt.Errorf(
				"cert-manager is required for internal TLS but its Certificate CRD is not installed: %w",
				err,
			)
		}
		return err
	}
	return nil
}

// buildCertificate mirrors certs.Build without an owner: the operator outlives
// every cluster, so nothing should garbage-collect its identity.
func buildCertificate(namespace, issuerName, certificateName string) *unstructured.Unstructured {
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certs.GVK)
	cert.SetName(certificateName)
	cert.SetNamespace(namespace)
	cert.Object["spec"] = map[string]any{
		"secretName": certificateName,
		"dnsNames":   []any{},
		"duration":   certs.Duration,
		"literalSubject": fmt.Sprintf(
			certs.LiteralSubjectTemplate,
			certs.TruncateCommonName(
				"multigres-operator."+namespace+"."+multigresv1alpha1.InternalTLSIdentityDomain,
			),
		),
		"issuerRef": map[string]any{
			"name":  issuerName,
			"kind":  "ClusterIssuer",
			"group": "cert-manager.io",
		},
		"privateKey": map[string]any{
			"algorithm": "RSA",
			"size":      int64(2048),
		},
		"usages": []any{"digital signature", "key encipherment", "client auth"},
	}
	return cert
}

// certificateNameForIssuer preserves the original name for the configured
// default and uses a stable DNS-safe suffix for every additional issuer.
func certificateNameForIssuer(issuerName, defaultIssuerName string) string {
	if issuerName == defaultIssuerName {
		return CertificateName
	}
	sum := sha256.Sum256([]byte(issuerName))
	return fmt.Sprintf("%s-%x", CertificateName, sum[:6])
}

func buildTLSConfig(secret *corev1.Secret) (*tls.Config, error) {
	certPEM, ok := secret.Data[corev1.TLSCertKey]
	if !ok || len(certPEM) == 0 {
		return nil, fmt.Errorf("missing %q", corev1.TLSCertKey)
	}
	keyPEM, ok := secret.Data[corev1.TLSPrivateKeyKey]
	if !ok || len(keyPEM) == 0 {
		return nil, fmt.Errorf("missing %q", corev1.TLSPrivateKeyKey)
	}
	caPEM, ok := secret.Data[corev1.ServiceAccountRootCAKey]
	if !ok || len(caPEM) == 0 {
		return nil, fmt.Errorf("missing %q", corev1.ServiceAccountRootCAKey)
	}

	keyPair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parsing client key pair: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parsing %q", corev1.ServiceAccountRootCAKey)
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{keyPair},
		RootCAs:      roots,
		// Pooler SANs are per cluster (multipooler.<cluster>.<ns>.<domain>) and
		// this one client dials every cluster, so hostname verification is
		// replaced by chain-to-CA plus a multipooler-role SAN check.
		InsecureSkipVerify: true, //nolint:gosec // G402: VerifyConnection below enforces chain + SAN checks.
		VerifyConnection:   verifyPoolerConnection(roots),
	}, nil
}

func verifyPoolerConnection(roots *x509.CertPool) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("multipooler presented no certificate")
		}
		leaf := cs.PeerCertificates[0]
		intermediates := x509.NewCertPool()
		for _, c := range cs.PeerCertificates[1:] {
			intermediates.AddCert(c)
		}
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots:         roots,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}); err != nil {
			return fmt.Errorf("verifying multipooler certificate chain: %w", err)
		}
		if !hasPoolerSAN(leaf) {
			return fmt.Errorf(
				"multipooler certificate SANs %v carry no %s.*.%s identity",
				leaf.DNSNames,
				multigresv1alpha1.ComponentMultiPoolerTLS,
				multigresv1alpha1.InternalTLSIdentityDomain,
			)
		}
		return nil
	}
}

func hasPoolerSAN(leaf *x509.Certificate) bool {
	prefix := multigresv1alpha1.ComponentMultiPoolerTLS + "."
	suffix := "." + multigresv1alpha1.InternalTLSIdentityDomain
	for _, name := range leaf.DNSNames {
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix) &&
			len(name) > len(prefix)+len(suffix) {
			return true
		}
	}
	return false
}
