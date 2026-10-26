package toposerver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

type etcdMaintenanceClient interface {
	Status(context.Context, string) (*clientv3.StatusResponse, error)
	Members(context.Context) (*clientv3.MemberListResponse, error)
	Health(context.Context, string) error
	MoveLeader(context.Context, string, uint64) error
	Defragment(context.Context, string) error
	Close()
}

// Each endpoint has a separate client. A linearizable health read must reach
// that member, not silently fail over to a healthy endpoint in a client pool.
type memberClients struct {
	clients map[string]*clientv3.Client
	first   *clientv3.Client
}

func (r *TopoServerReconciler) maintenanceClient(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
) (etcdMaintenanceClient, error) {
	if r.newMaintenanceClient != nil {
		return r.newMaintenanceClient(ctx, ts)
	}
	tlsConfig, err := r.maintenanceTLSConfig(ctx, ts)
	if err != nil {
		return nil, err
	}
	result := &memberClients{clients: make(map[string]*clientv3.Client)}
	for _, endpoint := range maintenanceEndpoints(ts) {
		c, err := clientv3.New(r.maintenanceClientConfig(ctx, endpoint, tlsConfig))
		if err != nil {
			result.Close()
			return nil, err
		}
		result.clients[endpoint] = c
		if result.first == nil {
			result.first = c
		}
	}
	return result, nil
}

// maintenanceClientConfig builds the clientv3.Config for a single etcd member.
func (r *TopoServerReconciler) maintenanceClientConfig(
	ctx context.Context,
	endpoint string,
	tlsConfig *tls.Config,
) clientv3.Config {
	cfg := clientv3.Config{
		Endpoints:   []string{endpoint},
		TLS:         tlsConfig,
		DialTimeout: 5 * time.Second,
		Context:     ctx,
	}
	if r.DialContext == nil {
		return cfg
	}
	// clientv3 (v3.7) sets no dialer of its own and appends Config.DialOptions
	// last, so this one carries every member connection.
	cfg.DialOptions = []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return r.DialContext(ctx, "tcp", addr)
		}),
	}
	return cfg
}

func (c *memberClients) Status(
	ctx context.Context,
	endpoint string,
) (*clientv3.StatusResponse, error) {
	return c.clients[endpoint].Status(ctx, endpoint)
}

func (c *memberClients) Members(ctx context.Context) (*clientv3.MemberListResponse, error) {
	return c.first.MemberList(ctx)
}

func (c *memberClients) Health(ctx context.Context, endpoint string) error {
	// Get is linearizable by default. This probe never writes a health key.
	_, err := c.clients[endpoint].Get(
		ctx,
		"/multigres-operator/maintenance-health",
		clientv3.WithCountOnly(),
	)
	return err
}

func (c *memberClients) MoveLeader(ctx context.Context, endpoint string, id uint64) error {
	_, err := c.clients[endpoint].MoveLeader(ctx, id)
	return err
}

func (c *memberClients) Defragment(ctx context.Context, endpoint string) error {
	_, err := c.clients[endpoint].Defragment(ctx, endpoint)
	return err
}

func (c *memberClients) Close() {
	for _, c := range c.clients {
		_ = c.Close()
	}
}

func (r *TopoServerReconciler) maintenanceTLSConfig(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
) (*tls.Config, error) {
	if !ts.Spec.TLS.IsEnabled() {
		return nil, nil
	}
	secret := &corev1.Secret{}
	key := client.ObjectKey{
		Namespace: ts.Namespace,
		Name:      multigresv1alpha1.TopoServerCertSecretName(ts.Name),
	}
	if err := r.maintenanceReader().Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("reading etcd maintenance credential: %w", err)
	}
	// The managed serving identity also has client-auth usage for peer TLS.
	cert, err := tls.X509KeyPair(
		secret.Data[corev1.TLSCertKey],
		secret.Data[corev1.TLSPrivateKeyKey],
	)
	if err != nil {
		return nil, fmt.Errorf("loading etcd maintenance certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data["ca.crt"]) {
		return nil, fmt.Errorf("etcd maintenance Secret %q has no valid ca.crt", key.Name)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      roots,
		Certificates: []tls.Certificate{cert},
	}, nil
}
