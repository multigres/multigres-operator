// Package topo provides utility functions for interacting with the Multigres
// global topology server (etcd). It consolidates store creation and common
// topology operations (database/cell registration, pooler queries) that were
// previously duplicated across data-handler controllers.
package topo

import (
	"context"
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/topoclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// tls Secret keys follow the cert-manager layout: the client keypair in
// tls.crt and tls.key, and the CA bundle in ca.crt.
const (
	tlsCertKey = "tls.crt"
	tlsKeyKey  = "tls.key"
	tlsCAKey   = "ca.crt"
)

// NewStoreFromShard creates a topoclient.Store using the GlobalTopoServer
// configuration from a Shard resource. When the reference names a client
// credential Secret, the connection presents that certificate; otherwise it
// connects in plaintext.
func NewStoreFromShard(
	ctx context.Context,
	c client.Reader,
	shard *multigresv1alpha1.Shard,
) (topoclient.Store, error) {
	return newStore(ctx, c, shard.Namespace, shard.Spec.GlobalTopoServer)
}

// NewStoreFromCell creates a topoclient.Store using the GlobalTopoServer
// configuration from a Cell resource.
func NewStoreFromCell(
	ctx context.Context,
	c client.Reader,
	cell *multigresv1alpha1.Cell,
) (topoclient.Store, error) {
	return newStore(ctx, c, cell.Namespace, cell.Spec.GlobalTopoServer)
}

// NewStoreFromRef creates a topoclient.Store using a GlobalTopoServerRef and the
// namespace the referenced Secrets live in. This is used by the MultigresCluster
// controller, which already computes the reference.
func NewStoreFromRef(
	ctx context.Context,
	c client.Reader,
	namespace string,
	ref multigresv1alpha1.GlobalTopoServerRef,
) (topoclient.Store, error) {
	return newStore(ctx, c, namespace, ref)
}

// newStore opens a topology store for the given reference, loading client TLS
// material from the referenced Secrets when the reference carries it.
func newStore(
	ctx context.Context,
	c client.Reader,
	namespace string,
	ref multigresv1alpha1.GlobalTopoServerRef,
) (topoclient.Store, error) {
	implementation := ref.Implementation
	if implementation == "" {
		implementation = "etcd"
	}

	config := topoclient.NewDefaultTopoConfig()
	tlsOptions, err := loadClientTLS(ctx, c, namespace, ref)
	if err != nil {
		return nil, err
	}
	config.TLS = tlsOptions

	store, err := topoclient.OpenServer(
		implementation, ref.RootPath, []string{ref.Address}, config,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open topology store: %w", err)
	}
	return store, nil
}

// loadClientTLS reads the client keypair and CA bundle the reference names and
// returns them as topoclient.TLSOptions. It returns nil when the reference
// carries no TLS material, so an unconfigured connection stays plaintext. A
// named Secret that is missing or lacks a required key is a hard error that
// names the Secret, so a misconfigured cluster fails visibly rather than
// silently connecting without a certificate.
func loadClientTLS(
	ctx context.Context,
	c client.Reader,
	namespace string,
	ref multigresv1alpha1.GlobalTopoServerRef,
) (*topoclient.TLSOptions, error) {
	if ref.ClientCertSecret == "" && ref.CASecret == "" {
		return nil, nil
	}

	opts := &topoclient.TLSOptions{}
	secrets := map[string]*corev1.Secret{}

	get := func(name string) (*corev1.Secret, error) {
		if s, ok := secrets[name]; ok {
			return s, nil
		}
		s := &corev1.Secret{}
		key := types.NamespacedName{Namespace: namespace, Name: name}
		if err := c.Get(ctx, key, s); err != nil {
			return nil, fmt.Errorf(
				"reading topology client TLS Secret %q in namespace %q: %w",
				name, namespace, err,
			)
		}
		secrets[name] = s
		return s, nil
	}

	if ref.ClientCertSecret != "" {
		s, err := get(ref.ClientCertSecret)
		if err != nil {
			return nil, err
		}
		cert, err := requireKey(s, tlsCertKey)
		if err != nil {
			return nil, err
		}
		keyPEM, err := requireKey(s, tlsKeyKey)
		if err != nil {
			return nil, err
		}
		opts.CertPEM = cert
		opts.KeyPEM = keyPEM
	}

	if ref.CASecret != "" {
		s, err := get(ref.CASecret)
		if err != nil {
			return nil, err
		}
		ca, err := requireKey(s, tlsCAKey)
		if err != nil {
			return nil, err
		}
		opts.CAPEM = ca
	}

	return opts, nil
}

// requireKey returns the named key's bytes, or an error naming the Secret and
// the missing key.
func requireKey(secret *corev1.Secret, key string) ([]byte, error) {
	v, ok := secret.Data[key]
	if !ok || len(v) == 0 {
		return nil, fmt.Errorf(
			"topology client TLS Secret %q in namespace %q is missing key %q",
			secret.Name, secret.Namespace, key,
		)
	}
	return v, nil
}

// IsTopoUnavailable returns true if the error indicates the topology server
// is not reachable (e.g., gRPC UNAVAILABLE during startup).
func IsTopoUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNAVAILABLE") ||
		strings.Contains(msg, "no connection available")
}
