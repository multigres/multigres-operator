// Package poolerclient resolves the multipooler RPC client a shard must use:
// a cluster-bound mTLS identity for InternalTLS clusters, insecure otherwise.
package poolerclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	defaultRefreshInterval = 5 * time.Minute
	// Cache initial credential failures until the shard's normal retry. This
	// prevents every shard in a cluster from repeating the same uncached Secret
	// read while cert-manager is still issuing the certificate.
	defaultFailureRetryInterval = 10 * time.Second
	// Data-plane reconciles are bounded to 30 seconds. Keep the old generation
	// alive for twice that deadline so rotation cannot interrupt an in-flight RPC.
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
	Capacity int
	Insecure rpcclient.MultipoolerClient
	// RPCOptions apply to every mTLS client the resolver creates, e.g. a
	// context dialer in laptop mode.
	RPCOptions []rpcclient.Option
}

// OperatorCertResolver caches one mTLS MultipoolerClient per cluster. The
// owning MultigresCluster controller creates and owns each client credential.
type OperatorCertResolver struct {
	reader client.Reader
	opts   Options

	refreshInterval time.Duration
	failureRetry    time.Duration
	retirementGrace time.Duration
	newClient       func(*tls.Config) rpcclient.MultipoolerClient

	mu     sync.Mutex
	states map[types.NamespacedName]*clusterState
	// active is maintained by the cluster reconciler. Requiring membership
	// prevents shards delayed by garbage collection from recreating client
	// state after their owning cluster was deleted, without retaining a
	// tombstone for every deleted cluster name.
	active map[types.NamespacedName]struct{}
	closed bool
}

type clusterState struct {
	mu              sync.Mutex
	closed          bool
	refreshing      bool
	refreshDone     chan struct{}
	tlsClient       rpcclient.MultipoolerClient
	retiredClients  []*retiredClient
	resourceVersion string
	fetchedAt       time.Time
	lastErr         error
}

type retiredClient struct {
	client rpcclient.MultipoolerClient
	timer  *time.Timer
}

// NewOperatorCertResolver creates a resolver. reader must be uncached because
// cert-manager Secrets do not carry the label required by the filtered cache.
func NewOperatorCertResolver(
	reader client.Reader,
	opts Options,
) (*OperatorCertResolver, error) {
	if reader == nil {
		return nil, fmt.Errorf("operator internal TLS resolver requires a Secret reader")
	}
	if opts.Insecure == nil {
		return nil, fmt.Errorf("operator internal TLS resolver requires an insecure client")
	}
	r := &OperatorCertResolver{
		reader:          reader,
		opts:            opts,
		refreshInterval: defaultRefreshInterval,
		failureRetry:    defaultFailureRetryInterval,
		retirementGrace: defaultRetirementGracePeriod,
		states:          make(map[types.NamespacedName]*clusterState),
		active:          make(map[types.NamespacedName]struct{}),
	}
	r.newClient = func(tlsConfig *tls.Config) rpcclient.MultipoolerClient {
		return rpcclient.NewMultipoolerClient(
			r.opts.Capacity,
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			r.opts.RPCOptions...,
		)
	}
	return r, nil
}

// ClientFor implements Resolver.
func (r *OperatorCertResolver) ClientFor(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (rpcclient.MultipoolerClient, error) {
	if !shard.Spec.InternalTLS.IsEnabled() {
		return r.opts.Insecure, nil
	}

	clusterKey, err := clusterKeyForShard(shard)
	if err != nil {
		return nil, err
	}
	if err := r.ensureClusterActive(ctx, clusterKey); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, fmt.Errorf("operator internal TLS resolver is closed")
	}
	state := r.states[clusterKey]
	if state == nil {
		state = &clusterState{}
		r.states[clusterKey] = state
	}
	r.mu.Unlock()

	for {
		state.mu.Lock()
		if state.closed {
			state.mu.Unlock()
			return nil, fmt.Errorf("operator internal TLS resolver is closed")
		}

		now := time.Now()
		hadClient := state.tlsClient != nil
		if hadClient {
			nextRefresh := r.refreshInterval
			if state.lastErr != nil {
				nextRefresh = r.failureRetry
			}
			if state.refreshing || now.Sub(state.fetchedAt) < nextRefresh {
				current := state.tlsClient
				state.mu.Unlock()
				return current, nil
			}
			// Refresh outside the lock. Other reconciles can continue using the
			// current generation while the uncached API read is in flight.
			state.refreshing = true
			state.refreshDone = make(chan struct{})
			state.mu.Unlock()
		} else {
			if state.refreshing {
				// Join the in-flight cold initialization. The first caller performs
				// the API read; all other shard reconciles reuse its result.
				done := state.refreshDone
				state.mu.Unlock()
				select {
				case <-done:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				state.mu.Lock()
				if state.closed {
					state.mu.Unlock()
					return nil, fmt.Errorf("operator internal TLS resolver is closed")
				}
				if state.tlsClient != nil {
					current := state.tlsClient
					state.mu.Unlock()
					return current, nil
				}
				if state.lastErr != nil {
					err := state.lastErr
					state.mu.Unlock()
					return nil, err
				}
				state.mu.Unlock()
				// The initialization leader may have been canceled. Its context error
				// is caller-local and must not poison healthy waiters, so compete to
				// become the next leader instead of manufacturing a cached failure.
				continue
			}
			if state.lastErr != nil && now.Sub(state.fetchedAt) < r.failureRetry {
				err := state.lastErr
				state.mu.Unlock()
				return nil, err
			}
			state.refreshing = true
			state.refreshDone = make(chan struct{})
			state.mu.Unlock()
		}

		secretKey := types.NamespacedName{
			Namespace: clusterKey.Namespace,
			Name: multigresv1alpha1.ComponentCertSecretName(
				multigresv1alpha1.ComponentOperatorTLS,
				clusterKey.Name,
				clusterKey.Namespace,
			),
		}
		secret := &corev1.Secret{}
		readErr := r.reader.Get(ctx, secretKey, secret)
		readCompletedAt := time.Now()
		state.mu.Lock()
		state.refreshing = false
		done := state.refreshDone
		state.refreshDone = nil
		defer close(done)
		if state.closed {
			state.mu.Unlock()
			return nil, fmt.Errorf("operator internal TLS resolver is closed")
		}
		defer state.mu.Unlock()

		if readErr != nil {
			return r.keepExistingOrError(
				ctx, state, readCompletedAt,
				fmt.Errorf("reading operator internal TLS secret %s: %w", secretKey, readErr),
			)
		}

		if state.tlsClient != nil && secret.ResourceVersion == state.resourceVersion {
			state.fetchedAt = readCompletedAt
			state.lastErr = nil
			return state.tlsClient, nil
		}

		serverName := multigresv1alpha1.ComponentCertCommonName(
			multigresv1alpha1.ComponentMultiPoolerTLS,
			clusterKey.Name,
			clusterKey.Namespace,
		)
		tlsConfig, err := buildTLSConfig(secret, serverName)
		if err != nil {
			return r.keepExistingOrError(
				ctx, state, readCompletedAt,
				fmt.Errorf("building TLS config from secret %s: %w", secretKey, err),
			)
		}

		if state.tlsClient != nil {
			r.retireClient(state, state.tlsClient)
		}
		state.tlsClient = r.newClient(tlsConfig)
		state.resourceVersion = secret.ResourceVersion
		state.fetchedAt = readCompletedAt
		state.lastErr = nil
		return state.tlsClient, nil
	}
}

// ensureClusterActive validates an inactive cluster directly against the API.
// Controller startup ordering is not guaranteed, so a shard may reconcile
// before the cluster controller has populated the lifecycle registry.
func (r *OperatorCertResolver) ensureClusterActive(
	ctx context.Context,
	key types.NamespacedName,
) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return fmt.Errorf("operator internal TLS resolver is closed")
	}
	_, active := r.active[key]
	r.mu.Unlock()
	if active {
		return nil
	}

	cluster := &multigresv1alpha1.MultigresCluster{}
	if err := r.reader.Get(ctx, key, cluster); err != nil {
		return fmt.Errorf("validating MultigresCluster %s: %w", key, err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return fmt.Errorf("MultigresCluster %s is being deleted", key)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("operator internal TLS resolver is closed")
	}
	r.active[key] = struct{}{}
	return nil
}

func clusterKeyForShard(shard *multigresv1alpha1.Shard) (types.NamespacedName, error) {
	clusterName := shard.Labels[metadata.LabelMultigresCluster]
	if clusterName == "" {
		return types.NamespacedName{}, fmt.Errorf(
			"internal TLS shard %s/%s has no %s label",
			shard.Namespace,
			shard.Name,
			metadata.LabelMultigresCluster,
		)
	}
	return types.NamespacedName{Namespace: shard.Namespace, Name: clusterName}, nil
}

// keepExistingOrError is called with state.mu held. Once a client has been
// built, credential refresh is best-effort: transient reads or a temporarily
// malformed rotating Secret must not interrupt working RPCs.
func (r *OperatorCertResolver) keepExistingOrError(
	ctx context.Context,
	state *clusterState,
	now time.Time,
	err error,
) (rpcclient.MultipoolerClient, error) {
	if ctx.Err() != nil {
		// Cancellation belongs to this reconcile. Do not cache it for another
		// caller and do not delay a credential refresh by the normal interval.
		if state.tlsClient != nil {
			return state.tlsClient, nil
		}
		return nil, err
	}
	if state.tlsClient == nil {
		state.fetchedAt = now
		state.lastErr = err
		return nil, err
	}
	// A working client remains usable, but retry the failed refresh on the
	// short failure interval rather than waiting for the full refresh period.
	state.fetchedAt = now
	state.lastErr = err
	log.FromContext(ctx).Error(
		err,
		"Unable to refresh operator internal TLS client; keeping current client",
	)
	return state.tlsClient, nil
}

// ActivateCluster allows a live cluster's shards to create client state. It is
// deliberately separate from ClientFor so a stale shard reconcile after
// deletion cannot undo ForgetCluster.
func (r *OperatorCertResolver) ActivateCluster(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.active[key] = struct{}{}
	}
}

// ForgetCluster removes a deleted cluster's cached client. The current client
// receives the same grace period as a rotated client so deletion cannot cut
// off an RPC that was already in flight.
func (r *OperatorCertResolver) ForgetCluster(key types.NamespacedName) {
	r.mu.Lock()
	state := r.states[key]
	delete(r.states, key)
	delete(r.active, key)
	r.mu.Unlock()
	if state == nil {
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return
	}
	state.closed = true
	if state.tlsClient != nil {
		r.retireClient(state, state.tlsClient)
		state.tlsClient = nil
	}
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
	states := make([]*clusterState, 0, len(r.states))
	for _, state := range r.states {
		states = append(states, state)
	}
	r.states = nil
	r.active = nil
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
	state *clusterState,
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

func buildTLSConfig(secret *corev1.Secret, serverName string) (*tls.Config, error) {
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
		ServerName:   serverName,
	}, nil
}
