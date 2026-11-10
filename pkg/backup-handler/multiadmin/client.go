// Package multiadmin is the backup controllers' client for a cluster's
// multiadmin gRPC service. It exposes only the two RPCs the attempt controller
// needs, so tests can substitute the in-memory Fake.
package multiadmin

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
)

// GRPCPort is multiadmin's gRPC port, as exposed by the <cluster>-multiadmin
// Service.
const GRPCPort = 18070

// Client is the subset of MultiadminService the backup controllers call.
type Client interface {
	Backup(
		ctx context.Context,
		req *multiadminpb.BackupRequest,
		opts ...grpc.CallOption,
	) (*multiadminpb.BackupResponse, error)
	GetBackupJobStatus(
		ctx context.Context,
		req *multiadminpb.GetBackupJobStatusRequest,
		opts ...grpc.CallOption,
	) (*multiadminpb.GetBackupJobStatusResponse, error)
}

// Factory returns the Client for a cluster.
type Factory interface {
	ClientFor(ctx context.Context, cluster *multigresv1alpha1.MultigresCluster) (Client, error)
}

// Address returns the in-cluster gRPC address of a cluster's multiadmin.
func Address(cluster *multigresv1alpha1.MultigresCluster) string {
	return fmt.Sprintf("%s-multiadmin.%s.svc:%d", cluster.Name, cluster.Namespace, GRPCPort)
}

// GRPCFactory dials multiadmin over gRPC and caches one connection per
// cluster. Clusters with internal TLS are dialed with the operator's
// per-cluster client certificate, the same identity used for multipoolers.
type GRPCFactory struct {
	// Reader must be uncached: cert-manager Secrets do not carry the label the
	// operator's filtered Secret cache requires.
	Reader client.Reader
	// DialContext, when set, replaces the default dialer (laptop mode).
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)

	mu    sync.Mutex
	conns map[types.NamespacedName]*cachedConn
}

type cachedConn struct {
	// version identifies the credentials the connection was built with:
	// "insecure", or the operator TLS Secret's resourceVersion.
	version string
	conn    *grpc.ClientConn
	client  multiadminpb.MultiadminServiceClient
}

// retireGrace keeps a replaced connection open long enough for in-flight
// RPCs to finish.
const retireGrace = time.Minute

// ClientFor implements Factory.
func (f *GRPCFactory) ClientFor(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (Client, error) {
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}

	version := "insecure"
	creds := insecure.NewCredentials()
	if cluster.Spec.InternalTLS.IsEnabled() {
		secret := &corev1.Secret{}
		secretKey := types.NamespacedName{
			Namespace: cluster.Namespace,
			Name: multigresv1alpha1.ComponentCertSecretName(
				multigresv1alpha1.ComponentOperatorTLS, cluster.Name, cluster.Namespace,
			),
		}
		if err := f.Reader.Get(ctx, secretKey, secret); err != nil {
			return nil, fmt.Errorf("reading operator TLS secret %s: %w", secretKey, err)
		}
		version = "tls/" + secret.ResourceVersion
		f.mu.Lock()
		cached := f.conns[key]
		f.mu.Unlock()
		if cached != nil && cached.version == version {
			return cached.client, nil
		}
		tlsConfig, err := poolerclient.ClientTLSConfig(
			secret,
			multigresv1alpha1.ComponentCertCommonName(
				multigresv1alpha1.ComponentMultiAdminTLS, cluster.Name, cluster.Namespace,
			),
		)
		if err != nil {
			return nil, fmt.Errorf("building TLS config from secret %s: %w", secretKey, err)
		}
		creds = credentials.NewTLS(tlsConfig)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conns == nil {
		f.conns = make(map[types.NamespacedName]*cachedConn)
	}
	if cached := f.conns[key]; cached != nil {
		if cached.version == version {
			return cached.client, nil
		}
		old := cached.conn
		time.AfterFunc(retireGrace, func() { _ = old.Close() })
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if f.DialContext != nil {
		dial := f.DialContext
		opts = append(opts, grpc.WithContextDialer(
			func(ctx context.Context, addr string) (net.Conn, error) {
				return dial(ctx, "tcp", addr)
			},
		))
	}
	conn, err := grpc.NewClient(Address(cluster), opts...)
	if err != nil {
		return nil, fmt.Errorf("creating multiadmin client for %s: %w", key, err)
	}
	cached := &cachedConn{
		version: version,
		conn:    conn,
		client:  multiadminpb.NewMultiadminServiceClient(conn),
	}
	f.conns[key] = cached
	return cached.client, nil
}

// Forget closes and drops a deleted cluster's connection.
func (f *GRPCFactory) Forget(key types.NamespacedName) {
	f.mu.Lock()
	cached := f.conns[key]
	delete(f.conns, key)
	f.mu.Unlock()
	if cached != nil {
		time.AfterFunc(retireGrace, func() { _ = cached.conn.Close() })
	}
}

// Close closes every cached connection.
func (f *GRPCFactory) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, cached := range f.conns {
		_ = cached.conn.Close()
		delete(f.conns, key)
	}
}
