//go:build integration

package toposerver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres/go/common/topoclient"
	_ "github.com/multigres/multigres/go/common/topoclient/etcdtopo"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/client-go/tools/record"
)

// These tests launch disposable local etcd processes, never a configured
// Kubernetes cluster. Prefer the same binary as envtest when available.
func startMaintenanceEtcd(t *testing.T) (*memberClients, []string) {
	c, endpoints, _ := startTestEtcd(t)
	return c, endpoints
}

func startTestEtcd(t *testing.T) (*memberClients, []string, []func()) {
	t.Helper()
	binary := filepath.Join(os.Getenv("KUBEBUILDER_ASSETS"), "etcd")
	if _, err := os.Stat(binary); err != nil {
		var lookupErr error
		binary, lookupErr = exec.LookPath("etcd")
		if lookupErr != nil {
			t.Fatal("integration test requires etcd or KUBEBUILDER_ASSETS")
		}
	}
	allocateURL := func() string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		url := "http://" + listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		return url
	}
	endpoints, peers, cluster := make([]string, 3), make([]string, 3), make([]string, 3)
	for i := range endpoints {
		endpoints[i] = allocateURL()
		peers[i] = allocateURL()
		cluster[i] = fmt.Sprintf("member-%d=%s", i, peers[i])
	}
	stop := make([]func(), len(endpoints))
	for i := range endpoints {
		dir := t.TempDir()
		logFile, err := os.Create(filepath.Join(dir, "etcd.log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(
			binary,
			"--name",
			fmt.Sprintf("member-%d", i),
			"--data-dir",
			filepath.Join(dir, "data"),
			"--listen-client-urls",
			endpoints[i],
			"--advertise-client-urls",
			endpoints[i],
			"--listen-peer-urls",
			peers[i],
			"--initial-advertise-peer-urls",
			peers[i],
			"--initial-cluster",
			strings.Join(cluster, ","),
			"--initial-cluster-token",
			"maintenance-test",
			"--auto-compaction-mode",
			"periodic",
			"--auto-compaction-retention",
			"2s",
			"--quota-backend-bytes",
			"536870912",
			"--log-level",
			"error",
		)
		cmd.Stdout, cmd.Stderr = logFile, logFile
		if err := cmd.Start(); err != nil {
			_ = logFile.Close()
			t.Fatal(err)
		}
		stop[i] = func() { _ = cmd.Process.Kill() }
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			_ = logFile.Close()
			if t.Failed() {
				data, _ := os.ReadFile(filepath.Join(dir, "etcd.log"))
				t.Logf("etcd log: %s", data)
			}
		})
	}
	c := &memberClients{clients: map[string]*clientv3.Client{}}
	for _, ep := range endpoints {
		cl, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		c.clients[ep] = cl
		if c.first == nil {
			c.first = cl
		}
	}
	t.Cleanup(c.Close)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := healthyMembers(t.Context(), c, endpoints); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("etcd did not become healthy: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return c, endpoints, stop
}

func TestLiveEtcdCompactionAndMaintenance(t *testing.T) {
	c, endpoints := startMaintenanceEtcd(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	value := strings.Repeat("x", 128*1024)
	var firstSize int64
	for cycle := range 5 {
		first, err := c.first.Put(ctx, "/maintenance-test/data", value)
		if err != nil {
			t.Fatal(err)
		}
		for range 63 {
			if _, err := c.first.Put(ctx, "/maintenance-test/data", value); err != nil {
				t.Fatal(err)
			}
		}
		deadline := time.Now().Add(15 * time.Second)
		for {
			_, err := c.first.Get(
				ctx,
				"/maintenance-test/data",
				clientv3.WithRev(first.Header.Revision),
			)
			if errors.Is(err, rpctypes.ErrCompacted) {
				break
			}
			if err != nil || time.Now().After(deadline) {
				t.Fatalf("history was not compacted: %v", err)
			}
			time.Sleep(200 * time.Millisecond)
		}
		statuses, err := healthyMembers(ctx, c, endpoints)
		if err != nil {
			t.Fatal(err)
		}
		s := statuses[endpoints[0]]
		if cycle == 0 {
			firstSize = s.DbSize
		}
		t.Logf(
			"cycle %d: backend=%d in-use=%d revision=%d",
			cycle,
			s.DbSize,
			s.DbSizeInUse,
			s.Header.Revision,
		)
		if cycle == 4 && s.DbSize > firstSize+16*1024*1024 {
			t.Fatalf(
				"backend kept growing across compaction cycles: initial=%d final=%d",
				firstSize,
				s.DbSize,
			)
		}
	}

	// Maintenance probes and converged registration must not manufacture MVCC writes.
	store, err := topoclient.OpenServer(
		"etcd",
		"/operator-test",
		endpoints,
		topoclient.NewDefaultTopoConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	owner := &multigresv1alpha1.MultigresCluster{}
	register := func() {
		t.Helper()
		if err := topo.RegisterDatabaseFromSpec(
			ctx,
			store,
			record.NewFakeRecorder(10),
			owner,
			multigresv1alpha1.DatabaseConfig{Name: "db"},
			[]string{"cell1"},
			nil,
			"",
		); err != nil {
			t.Fatal(err)
		}
	}
	register()
	before, err := c.first.Get(ctx, "/operator-test", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		register()
		if _, err := healthyMembers(ctx, c, endpoints); err != nil {
			t.Fatal(err)
		}
	}
	after, err := c.first.Get(ctx, "/operator-test", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if before.Header.Revision != after.Header.Revision {
		t.Fatalf(
			"no-op reconciles advanced etcd revision: %d -> %d",
			before.Header.Revision,
			after.Header.Revision,
		)
	}

	// Reclaim each member independently, transferring leadership first where
	// needed and requiring healthy, linearizable reads between every operation.
	for _, ep := range endpoints {
		statuses, err := healthyMembers(ctx, c, endpoints)
		if err != nil {
			t.Fatal(err)
		}
		s := statuses[ep]
		if s.Header.MemberId == s.Leader {
			for _, other := range endpoints {
				if other != ep {
					if err := c.MoveLeader(ctx, ep, statuses[other].Header.MemberId); err != nil {
						t.Fatal(err)
					}
					break
				}
			}
		}
		if err := c.Defragment(ctx, ep); err != nil {
			t.Fatal(err)
		}
		statuses, err = healthyMembers(ctx, c, endpoints)
		if err != nil {
			t.Fatal(err)
		}
		if statuses[ep].DbSize >= s.DbSize {
			t.Fatalf(
				"defragmentation did not shrink %s: %d -> %d",
				ep,
				s.DbSize,
				statuses[ep].DbSize,
			)
		}
	}
	got, err := c.first.Get(ctx, "/maintenance-test/data")
	if err != nil || len(got.Kvs) != 1 || string(got.Kvs[0].Value) != value {
		t.Fatalf("live data lost after maintenance: %v", err)
	}
}
