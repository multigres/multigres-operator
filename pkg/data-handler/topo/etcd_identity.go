package topo

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// etcdIdentityTimeout bounds one cluster-identity read, dial included.
const etcdIdentityTimeout = 10 * time.Second

// EtcdMember is one member of an etcd cluster as MemberList reports it.
type EtcdMember struct {
	ID         uint64
	Name       string
	PeerURLs   []string
	ClientURLs []string
}

// EtcdIdentity is what one etcd endpoint says about the cluster behind it:
// the membership MemberList returns and the cluster it was answered for, and
// the cluster and member that answered a Status on the same connection.
//
// The two halves are read together because a front end can answer one and
// forward the other: an etcd grpc-proxy invents the membership it returns (its
// own hostname, no peer URLs, ID 0) while Status reaches the real backend. A
// membership is only evidence about who serves an address when the member that
// answered Status is one of the members listed.
type EtcdIdentity struct {
	ClusterID       uint64
	Members         []EtcdMember
	StatusClusterID uint64
	StatusMemberID  uint64
}

// Implausible says why id is not the membership of a real etcd cluster, or
// "" when it is. A front end that answers MemberList itself (an etcd
// grpc-proxy lists only itself, with no peer URLs and ID 0) while forwarding
// Status fails it: the member that answered Status is not one it listed.
func (id EtcdIdentity) Implausible() string {
	switch {
	case id.ClusterID == 0:
		return "the membership carries no cluster ID"
	case id.StatusClusterID != id.ClusterID:
		return fmt.Sprintf("the membership is for cluster %x but Status answered for %x",
			id.ClusterID, id.StatusClusterID)
	case len(id.Members) == 0:
		return "the membership is empty"
	}
	answered := false
	for _, m := range id.Members {
		if m.ID == 0 || len(m.PeerURLs) == 0 {
			return fmt.Sprintf("member %q has no ID or no peer URL", m.Name)
		}
		answered = answered || m.ID == id.StatusMemberID
	}
	if !answered {
		return fmt.Sprintf(
			"the member that answered Status (%x) is not in the membership",
			id.StatusMemberID,
		)
	}
	return ""
}

// ReadEtcdIdentity reads ref's etcd identity using the same client TLS material
// NewStoreFromRef would. MemberList is linearizable (clientv3's default), so a
// partitioned minority member errors rather than answering from a stale view.
// A non-nil dial carries the connection (laptop mode); nil dials directly.
func ReadEtcdIdentity(
	ctx context.Context,
	c client.Reader,
	namespace string,
	ref multigresv1alpha1.GlobalTopoServerRef,
	dial func(ctx context.Context, network, addr string) (net.Conn, error),
) (EtcdIdentity, error) {
	if ref.Implementation != "" && ref.Implementation != "etcd" {
		return EtcdIdentity{}, fmt.Errorf(
			"topology implementation %q has no etcd identity",
			ref.Implementation,
		)
	}
	opts, err := loadClientTLS(ctx, c, namespace, ref)
	if err != nil {
		return EtcdIdentity{}, err
	}
	tlsConfig, err := etcdClientTLS(opts)
	if err != nil {
		return EtcdIdentity{}, fmt.Errorf("topology client TLS for %s: %w", ref.Address, err)
	}

	ctx, cancel := context.WithTimeout(ctx, etcdIdentityTimeout)
	defer cancel()
	cfg := clientv3.Config{
		Endpoints:   []string{ref.Address},
		TLS:         tlsConfig,
		DialTimeout: etcdIdentityTimeout,
		Context:     ctx,
	}
	if dial != nil {
		// clientv3 resolves endpoints itself and appends DialOptions last, so
		// dial receives the raw host:port of every member connection.
		cfg.DialOptions = []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				return dial(ctx, "tcp", addr)
			}),
		}
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return EtcdIdentity{}, fmt.Errorf("connecting to %s: %w", ref.Address, err)
	}
	defer func() { _ = cli.Close() }()

	members, err := cli.MemberList(ctx)
	if err != nil {
		return EtcdIdentity{}, fmt.Errorf("listing etcd members at %s: %w", ref.Address, err)
	}
	status, err := cli.Status(ctx, ref.Address)
	if err != nil {
		return EtcdIdentity{}, fmt.Errorf("reading etcd status from %s: %w", ref.Address, err)
	}
	id := EtcdIdentity{}
	if members.Header != nil {
		id.ClusterID = members.Header.ClusterId
	}
	if status.Header != nil {
		id.StatusClusterID = status.Header.ClusterId
		id.StatusMemberID = status.Header.MemberId
	}
	for _, m := range members.Members {
		id.Members = append(id.Members, EtcdMember{
			ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs,
		})
	}
	return id, nil
}

// etcdClientTLS builds the client TLS configuration from opts by the same rules
// as the topology store's own etcd client, so the identity read and the store
// reach the server the same way: no TLS without material, a keypair required
// once there is any, and the CA optional.
func etcdClientTLS(opts *topoclient.TLSOptions) (*tls.Config, error) {
	if opts == nil {
		return nil, nil
	}
	if len(opts.CertPEM) == 0 || len(opts.KeyPEM) == 0 {
		return nil, errors.New("a client certificate and key are required")
	}
	cert, err := tls.X509KeyPair(opts.CertPEM, opts.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("loading client certificate: %w", err)
	}
	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if len(opts.CAPEM) > 0 {
		config.RootCAs = x509.NewCertPool()
		if !config.RootCAs.AppendCertsFromPEM(opts.CAPEM) {
			return nil, errors.New("parsing CA certificate")
		}
	}
	return config, nil
}
