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

	"github.com/multigres/testkit/assert"
)

// These tests launch disposable local etcd processes, never a configured
// Kubernetes cluster. Prefer the same binary as envtest when available.
func startMaintenanceEtcd(t *testing.T) (*memberClients, []string) {
	t.Helper()
	ck := assert.NewAborting(t)
	binary := filepath.Join(os.Getenv("KUBEBUILDER_ASSETS"), "etcd")
	if _, err := os.Stat(binary); err != nil {
		var lookupErr error
		binary, lookupErr = exec.LookPath("etcd")
		ck.NoError(lookupErr, "integration test requires etcd or KUBEBUILDER_ASSETS")
	}
	allocateURL := func() string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		ck.NoError(err)
		url := "http://" + listener.Addr().String()
		ck.NoError(listener.Close())
		return url
	}
	endpoints, peers, cluster := make([]string, 3), make([]string, 3), make([]string, 3)
	for i := range endpoints {
		endpoints[i] = allocateURL()
		peers[i] = allocateURL()
		cluster[i] = fmt.Sprintf("member-%d=%s", i, peers[i])
	}
	for i := range endpoints {
		dir := t.TempDir()
		logFile, err := os.Create(filepath.Join(dir, "etcd.log"))
		ck.NoError(err)
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
		ck.NoError(err)
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
	return c, endpoints
}

func TestLiveEtcdCompactionAndMaintenance(t *testing.T) {
	ck := assert.NewAborting(t)
	c, endpoints := startMaintenanceEtcd(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	value := strings.Repeat("x", 128*1024)
	var firstSize int64
	for cycle := range 5 {
		first, err := c.first.Put(ctx, "/maintenance-test/data", value)
		ck.NoError(err)
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
			ck.False(err != nil || time.Now().After(deadline), "history was not compacted: %v", err)
			time.Sleep(200 * time.Millisecond)
		}
		statuses, err := healthyMembers(ctx, c, endpoints)
		ck.NoError(err)
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
	ck.NoError(err)
	defer func() { _ = store.Close() }()
	owner := &multigresv1alpha1.MultigresCluster{}
	register := func() {
		t.Helper()
		ck.NoError(topo.RegisterDatabaseFromSpec(
			ctx,
			store,
			record.NewFakeRecorder(10),
			owner,
			multigresv1alpha1.DatabaseConfig{Name: "db"},
			[]string{"cell1"},
			nil,
			"",
		))
	}
	register()
	before, err := c.first.Get(ctx, "/operator-test", clientv3.WithPrefix())
	ck.NoError(err)
	for range 5 {
		register()
		if _, err := healthyMembers(ctx, c, endpoints); err != nil {
			t.Fatal(err)
		}
	}
	after, err := c.first.Get(ctx, "/operator-test", clientv3.WithPrefix())
	ck.NoError(err)
	ck.Eq(after.Header.Revision, before.Header.Revision, "no-op reconciles advanced etcd revision")

	// Reclaim each member independently, transferring leadership first where
	// needed and requiring healthy, linearizable reads between every operation.
	for _, ep := range endpoints {
		statuses, err := healthyMembers(ctx, c, endpoints)
		ck.NoError(err)
		s := statuses[ep]
		if s.Header.MemberId == s.Leader {
			for _, other := range endpoints {
				if other != ep {
					ck.NoError(c.MoveLeader(ctx, ep, statuses[other].Header.MemberId))
					break
				}
			}
		}
		ck.NoError(c.Defragment(ctx, ep))
		statuses, err = healthyMembers(ctx, c, endpoints)
		ck.NoError(err)
		ck.Less(
			s.DbSize,
			statuses[ep].DbSize,
			"defragmentation did not shrink %s: %d ->",
			ep,
			s.DbSize,
		)
	}
	got, err := c.first.Get(ctx, "/maintenance-test/data")
	ck.False(
		err != nil || len(got.Kvs) != 1 || string(got.Kvs[0].Value) != value,
		"live data lost after maintenance: %v",
		err,
	)
}
