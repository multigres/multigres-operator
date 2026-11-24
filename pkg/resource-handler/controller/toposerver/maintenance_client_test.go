package toposerver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/topology"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMaintenanceClientDialOptionsFollowDialContext(t *testing.T) {
	withoutDialer := (&TopoServerReconciler{}).maintenanceClientConfig(
		t.Context(),
		[]string{"ep"},
		nil,
	)
	if len(withoutDialer.DialOptions) != 0 {
		t.Fatalf("nil DialContext: DialOptions = %d, want 0", len(withoutDialer.DialOptions))
	}
	if got := withoutDialer.Endpoints; len(got) != 1 || got[0] != "ep" {
		t.Fatalf("Endpoints = %v, want [ep]", got)
	}

	withDialer := (&TopoServerReconciler{
		DialContext: func(context.Context, string, string) (net.Conn, error) { return nil, nil },
	}).maintenanceClientConfig(
		t.Context(),
		[]string{"ep"},
		nil,
	)
	if len(withDialer.DialOptions) == 0 {
		t.Fatal("set DialContext: DialOptions is empty, want the context dialer")
	}
}

func TestMaintenanceTLSUsesUncachedCredentialAndFailsClosed(t *testing.T) {
	ts := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	adminKey := topology.AdminSecretKey(testOperatorNamespace, ts)
	admin := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: adminKey.Name, Namespace: adminKey.Namespace},
		Data: map[string][]byte{
			"tls.crt": certPEM,
			"tls.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
			"ca.crt":  certPEM,
		},
	}
	serving := admin.DeepCopy()
	serving.Name = multigresv1alpha1.TopoServerCertSecretName(ts.Name)
	serving.Namespace = ts.Namespace
	withAuth := func(ts *multigresv1alpha1.TopoServer) {
		ts.Spec.Auth = &multigresv1alpha1.TopoServerAuthSpec{Enabled: true}
	}
	invalid := func(secret *corev1.Secret) *corev1.Secret {
		result := secret.DeepCopy()
		delete(result.Data, corev1.TLSPrivateKeyKey)
		return result
	}
	for _, test := range []struct {
		name    string
		objects []runtime.Object
		adjust  func(*multigresv1alpha1.TopoServer)
		wantErr bool
	}{
		{name: "admin credential", objects: []runtime.Object{admin}, adjust: withAuth},
		{
			name:    "admin credential is not used without managed auth",
			objects: []runtime.Object{admin},
			wantErr: true,
		},
		{name: "serving credential during pre-auth upgrade", objects: []runtime.Object{serving}},
		{
			name:    "admin preferred over malformed serving credential",
			objects: []runtime.Object{admin, invalid(serving)},
			adjust:  withAuth,
		},
		{
			name:    "malformed admin does not fall back",
			objects: []runtime.Object{invalid(admin), serving},
			adjust:  withAuth,
			wantErr: true,
		},
		{
			name:    "auth requested does not fall back",
			objects: []runtime.Object{serving},
			adjust: func(ts *multigresv1alpha1.TopoServer) {
				ts.Spec.Auth = &multigresv1alpha1.TopoServerAuthSpec{Enabled: true}
			},
			wantErr: true,
		},
		{
			name:    "auth enabled status does not fall back",
			objects: []runtime.Object{serving},
			adjust:  func(ts *multigresv1alpha1.TopoServer) { ts.Status.AuthEnabled = true },
			wantErr: true,
		},
		{
			name:    "auth identity journal does not fall back",
			objects: []runtime.Object{serving},
			adjust: func(ts *multigresv1alpha1.TopoServer) {
				ts.Status.AuthIdentities = []string{"/multigres/project"}
			},
			wantErr: true,
		},
		{name: "missing key", objects: []runtime.Object{invalid(admin)}, adjust: withAuth, wantErr: true},
		{
			name: "invalid CA",
			objects: []runtime.Object{func() *corev1.Secret {
				s := admin.DeepCopy()
				s.Data["ca.crt"] = []byte("invalid")
				return s
			}()},
			adjust:  withAuth,
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := ts.DeepCopy()
			if test.adjust != nil {
				test.adjust(server)
			}
			r := &TopoServerReconciler{
				OperatorNamespace: testOperatorNamespace,
				Client:            fake.NewClientBuilder().WithScheme(certScheme()).Build(),
				APIReader: fake.NewClientBuilder().
					WithScheme(certScheme()).
					WithRuntimeObjects(test.objects...).
					Build(),
			}
			cfg, err := r.maintenanceTLSConfig(t.Context(), server)
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v", err)
			}
			if !test.wantErr &&
				(cfg.MinVersion < tls.VersionTLS12 || cfg.InsecureSkipVerify || cfg.RootCAs == nil || len(cfg.Certificates) != 1) {
				t.Fatal("invalid TLS configuration")
			}
		})
	}
}

type enabledAuthServer struct {
	etcdserverpb.UnimplementedAuthServer
}

func (enabledAuthServer) AuthStatus(
	context.Context,
	*etcdserverpb.AuthStatusRequest,
) (*etcdserverpb.AuthStatusResponse, error) {
	return &etcdserverpb.AuthStatusResponse{Enabled: true}, nil
}

func TestMaintenanceAuthReachesAnyMember(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	etcdserverpb.RegisterAuthServer(server, enabledAuthServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	ts := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "topo", Namespace: "infra"},
		Spec:       multigresv1alpha1.TopoServerSpec{Etcd: &multigresv1alpha1.EtcdSpec{}},
	}
	r := &TopoServerReconciler{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			if strings.HasPrefix(addr, ts.Name+"-0.") {
				return nil, errors.New("member 0 is unreachable")
			}
			return listener.DialContext(ctx)
		},
	}
	c, err := r.maintenanceClient(t.Context(), ts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	status, err := c.(etcdAuthClient).AuthStatus(ctx)
	if err != nil || !status.Enabled {
		t.Fatalf("auth status with member 0 down: %+v, %v", status, err)
	}
}
