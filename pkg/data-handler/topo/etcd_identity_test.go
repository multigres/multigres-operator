package topo_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
)

// startEtcd runs the etcd binary envtest ships with on free local ports and
// returns its client port. Skips when KUBEBUILDER_ASSETS is unset, as it is
// outside `make test`.
func startEtcd(t *testing.T) int {
	t.Helper()
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run through make test")
	}
	freePort := func() int {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		defer func() { _ = l.Close() }()
		return l.Addr().(*net.TCPAddr).Port
	}
	client, peer := freePort(), freePort()
	// The binary comes from the developer's own envtest install, the same one
	// envtest itself launches.
	//nolint:gosec // G204: path from KUBEBUILDER_ASSETS, arguments are local ports and a temp dir
	cmd := exec.CommandContext(t.Context(), filepath.Join(assets, "etcd"),
		"--data-dir", t.TempDir(),
		"--listen-client-urls", fmt.Sprintf("http://127.0.0.1:%d", client),
		"--advertise-client-urls", fmt.Sprintf("http://127.0.0.1:%d", client),
		"--listen-peer-urls", fmt.Sprintf("http://127.0.0.1:%d", peer),
		"--initial-advertise-peer-urls", fmt.Sprintf("http://127.0.0.1:%d", peer),
		"--initial-cluster", fmt.Sprintf("default=http://127.0.0.1:%d", peer),
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start etcd: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return client
}

// startGRPCProxy runs `etcd grpc-proxy` in front of the etcd at clientPort and
// returns its listen port. With --advertise-client-url set, the proxy answers
// MemberList itself rather than forwarding it, which is what makes it look
// like a different etcd to anything that trusts the membership alone.
func startGRPCProxy(t *testing.T, clientPort int) int {
	t.Helper()
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	//nolint:gosec // G204: path from KUBEBUILDER_ASSETS, arguments are local ports
	cmd := exec.CommandContext(t.Context(), filepath.Join(assets, "etcd"), "grpc-proxy", "start",
		"--endpoints", fmt.Sprintf("127.0.0.1:%d", clientPort),
		"--listen-addr", fmt.Sprintf("127.0.0.1:%d", port),
		"--advertise-client-url", fmt.Sprintf("127.0.0.1:%d", port),
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start grpc-proxy: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return port
}

func TestReadEtcdIdentity(t *testing.T) {
	a, b := startEtcd(t), startEtcd(t)
	proxy := startGRPCProxy(t, a)

	readWhenUp := func(address string) topo.EtcdIdentity {
		t.Helper()
		var last error
		for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
			id, err := topo.ReadEtcdIdentity(t.Context(), nil, "default",
				multigresv1alpha1.GlobalTopoServerRef{Address: address}, nil)
			if err == nil {
				return id
			}
			last = err
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("ReadEtcdIdentity(%s): %v", address, last)
		return topo.EtcdIdentity{}
	}

	viaIP := readWhenUp(fmt.Sprintf("http://127.0.0.1:%d", a))
	viaName := readWhenUp(fmt.Sprintf("http://localhost:%d", a))
	other := readWhenUp(fmt.Sprintf("http://127.0.0.1:%d", b))
	viaProxy := readWhenUp(fmt.Sprintf("http://127.0.0.1:%d", proxy))

	for name, id := range map[string]topo.EtcdIdentity{"a": viaIP, "b": other} {
		if bad := id.Implausible(); bad != "" {
			t.Errorf("etcd %s: a real etcd reads as implausible: %s", name, bad)
		}
	}
	if viaIP.ClusterID != viaName.ClusterID || len(viaName.Members) != 1 ||
		viaName.Members[0].ID != viaIP.Members[0].ID {
		t.Errorf("two addresses of one etcd disagree: %+v and %+v", viaIP, viaName)
	}
	if other.ClusterID == viaIP.ClusterID {
		t.Errorf("two separate etcds both reported cluster ID %x", other.ClusterID)
	}
	// The proxy forwards Status to etcd a but answers MemberList itself, so its
	// membership must not be believed.
	if viaProxy.StatusClusterID != viaIP.ClusterID {
		t.Errorf(
			"proxy Status answered for %x, want the backend's %x",
			viaProxy.StatusClusterID,
			viaIP.ClusterID,
		)
	}
	if viaProxy.Implausible() == "" {
		t.Errorf("a grpc-proxy's invented membership reads as plausible: %+v", viaProxy)
	}

	t.Run("refuses a non-etcd implementation", func(t *testing.T) {
		_, err := topo.ReadEtcdIdentity(t.Context(), nil, "default",
			multigresv1alpha1.GlobalTopoServerRef{
				Address:        fmt.Sprintf("http://127.0.0.1:%d", a),
				Implementation: "memory",
			}, nil)
		if err == nil {
			t.Error("want an error for a memory topology")
		}
	})
}

// In laptop mode the etcd behind a topology reference is reachable only through
// the laptop dialer, so the identity read must hand it the raw host:port, even
// for a host the local resolver cannot resolve.
func TestReadEtcdIdentityDialsThroughTheGivenDialer(t *testing.T) {
	var (
		mu    sync.Mutex
		dials []string
	)
	dial := func(_ context.Context, _, addr string) (net.Conn, error) {
		mu.Lock()
		dials = append(dials, addr)
		mu.Unlock()
		return nil, errors.New("recording dialer refuses every dial")
	}
	// The refused dial is retried until the context ends; one second is plenty
	// to observe the first attempt.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := topo.ReadEtcdIdentity(ctx, nil, "default",
		multigresv1alpha1.GlobalTopoServerRef{Address: "http://etcd-0.example.internal:2379"}, dial)
	if err == nil {
		t.Fatal("want an error from a dialer that refuses every dial")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dials) == 0 {
		t.Fatal("the dialer was never used")
	}
	if dials[0] != "etcd-0.example.internal:2379" {
		t.Errorf("dialed %q, want the raw host:port etcd-0.example.internal:2379", dials[0])
	}
}
