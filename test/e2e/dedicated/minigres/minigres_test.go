//go:build e2e

package minigres_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	metadata "github.com/multigres/multigres/go/pb/clustermetadata"
	manager "github.com/multigres/multigres/go/pb/multipoolermanager"
	data "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"
)

//go:embed testdata/etcd.yaml
var etcdManifest string

const fingerprintQuery = `SELECT count(*)::text || ':' || md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) FROM minigres_e2e`

// TestMinigresRuntime qualifies real Minigres, Postgres and etcd processes.
// Admin and topology connections use mTLS. This outage scenario uses plaintext
// SQL through the gateway Service; TestMinigresPublicTLS qualifies public SQL TLS.
func TestMinigresRuntime(t *testing.T) {
	cr, pod := startLiteRuntime(t, "")
	ns := cr.Namespace
	c, err := cluster.CRClient()
	require.NoError(t, err)
	sqlEventually(
		t,
		ns,
		pod.Name,
		"CREATE TABLE minigres_e2e (id integer PRIMARY KEY, payload text NOT NULL)",
		"CREATE TABLE",
	)
	sqlMust(t, ns, pod.Name, "INSERT INTO minigres_e2e VALUES (1, 'before outage')", "INSERT 0 1")
	before := fingerprint(t, ns, pod.Name)
	pvc := dataPVC(t, ns, pod)
	require.NotEmpty(t, pvc.Spec.VolumeName)
	for _, mode := range []string{"status", "reload", "plaintext", "no-client-cert", "wrong-server"} {
		admin(t, ns, pod.Name, mode)
	}
	_, err = kubectl(
		ns,
		"exec",
		pod.Name,
		"-c",
		"postgres",
		"--",
		"test",
		"-s",
		"/var/lib/pooler/topo-database.pb",
	)
	require.NoError(t, err, "bootstrap must persist topology on the data PVC")
	t.Logf(
		"bootstrap: pod=%s pvc=%s pv=%s fingerprint=%s",
		pod.UID,
		pvc.UID,
		pvc.Spec.VolumeName,
		before,
	)

	// Register restoration before stopping etcd, including on assertion failures.
	// Namespace diagnostics run after restoration and before dedicated teardown.
	t.Cleanup(func() {
		_, restoreErr := kubectl(ns, "scale", "deployment/external-etcd", "--replicas=1")
		if restoreErr != nil {
			t.Logf("restore etcd during cleanup: %v", restoreErr)
		}
	})
	scaleEtcd(t, ns, 0)
	eventually(t, "etcd process termination", func() (bool, error) { return etcdAbsent(ns) })
	t.Log("etcd stopped; testing 45 independent SQL write/read sessions without retries")
	start := time.Now()
	for id := 2; id <= 46; id++ {
		sqlMust(
			t,
			ns,
			pod.Name,
			fmt.Sprintf("INSERT INTO minigres_e2e VALUES (%d, 'during outage')", id),
			"INSERT 0 1",
		)
		sqlMust(t, ns, pod.Name, "SELECT count(*) FROM minigres_e2e", fmt.Sprint(id))
		time.Sleep(time.Second)
	}
	require.GreaterOrEqual(t, time.Since(start), 45*time.Second)
	admin(t, ns, pod.Name, "status")
	assertEtcdAbsent(t, ns)
	before = fingerprint(t, ns, pod.Name)

	// Stop just Minigres through the dedicated Kind node's container runtime.
	// This works even when the runtime image has no shell or kill executable.
	pod = getPod(t, ns, pod.Name)
	oldStatus := minigresStatus(t, pod)
	containerID := strings.TrimPrefix(oldStatus.ContainerID, "containerd://")
	require.NotEqual(t, oldStatus.ContainerID, containerID)
	require.Equal(t, cluster.Name+"-control-plane", pod.Spec.NodeName)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	// #nosec G204 -- Kind node name is asserted above; container ID comes from its pod status.
	out, err := exec.CommandContext(ctx, "docker", "exec", pod.Spec.NodeName,
		"crictl", "stop", "--timeout", "1", containerID).CombinedOutput()
	cancel()
	require.NoError(t, err, "%s", out)
	eventually(t, "Minigres container restart", func() (bool, error) {
		p, err := cluster.Clientset.CoreV1().
			Pods(ns).
			Get(t.Context(), pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		require.Equal(t, pod.UID, p.UID, "container restart must preserve the pod")
		return minigresStatus(t, p).RestartCount > oldStatus.RestartCount && ready(p), nil
	})
	sqlEventually(t, ns, pod.Name, fingerprintQuery, before)
	sqlMust(
		t,
		ns,
		pod.Name,
		"INSERT INTO minigres_e2e VALUES (100, 'after container restart')",
		"INSERT 0 1",
	)
	assertEtcdAbsent(t, ns)
	t.Log("container recovered offline with the same pod UID and all persisted rows")
	before = fingerprint(t, ns, pod.Name)

	require.NoError(
		t,
		cluster.Clientset.CoreV1().Pods(ns).Delete(t.Context(), pod.Name, metav1.DeleteOptions{}),
	)
	replacement := waitPod(t, ns, "lite", pod.UID)
	sqlEventually(t, ns, replacement.Name, fingerprintQuery, before)
	recoveredPVC := dataPVC(t, ns, replacement)
	require.Equal(t, pvc.UID, recoveredPVC.UID, "must reuse the original data PVC")
	require.Equal(
		t,
		pvc.Spec.VolumeName,
		recoveredPVC.Spec.VolumeName,
		"must reuse the original PV",
	)
	sqlMust(
		t,
		ns,
		replacement.Name,
		"INSERT INTO minigres_e2e VALUES (101, 'after pod recovery')",
		"INSERT 0 1",
	)
	assertEtcdAbsent(t, ns)
	admin(t, ns, replacement.Name, "status")
	t.Logf(
		"pod recovered offline: old=%s replacement=%s pvc=%s",
		pod.UID,
		replacement.UID,
		recoveredPVC.UID,
	)

	// A separate topology root and no PVC ensure this is truly a cold bootstrap.
	fresh := framework.MustLoadCluster("test/e2e/dedicated/minigres/testdata/cluster.yaml", ns)
	fresh.Name = "fresh"
	fresh.Spec.GlobalTopoServer.External = cr.Spec.GlobalTopoServer.External.DeepCopy()
	fresh.Spec.GlobalTopoServer.External.RootPath = "/multigres/" + ns + "/fresh/global"
	require.NoError(t, c.Create(t.Context(), fresh))
	eventually(t, "fresh cluster cell creation", func() (bool, error) {
		cells := &multigresv1alpha1.CellList{}
		err := c.List(
			t.Context(),
			cells,
			client.InNamespace(ns),
			client.MatchingLabels{"multigres.com/cluster": "fresh"},
		)
		return len(cells.Items) == 1, err
	})
	for range 15 {
		pods, err := cluster.Clientset.CoreV1().
			Pods(ns).
			List(t.Context(), metav1.ListOptions{LabelSelector: "multigres.com/cluster=fresh"})
		require.NoError(t, err)
		require.Empty(
			t,
			pods.Items,
			"cold bootstrap must not provision a pool pod while etcd is down",
		)
		time.Sleep(time.Second)
	}
	require.NoError(t, c.Delete(t.Context(), fresh))
	assertEtcdAbsent(t, ns)
	scaleEtcd(t, ns, 1)
	_, err = kubectl(ns, "rollout", "status", "deployment/external-etcd", "--timeout=120s")
	require.NoError(t, err)
	eventually(t, "topology re-registration at replacement pod IP", func() (bool, error) {
		return topologyRegistered(ns, replacement.Status.PodIP)
	})
	eventually(t, "healthy shard after topology recovery", func() (bool, error) {
		shards := &multigresv1alpha1.ShardList{}
		err := c.List(
			t.Context(),
			shards,
			client.InNamespace(ns),
			client.MatchingLabels{"multigres.com/cluster": "lite"},
		)
		return len(shards.Items) == 1 && shards.Items[0].Status.Phase == "Healthy", err
	})
	admin(t, ns, replacement.Name, "status")
	admin(t, ns, replacement.Name, "reload")
	sqlMust(
		t,
		ns,
		replacement.Name,
		"INSERT INTO minigres_e2e VALUES (102, 'after etcd return')",
		"INSERT 0 1",
	)
	sqlMust(t, ns, replacement.Name, "SELECT count(*) FROM minigres_e2e", "49")
	t.Logf("qualification complete: 49 rows, fingerprint=%s", fingerprint(t, ns, replacement.Name))
}

func kubectl(ns string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	// #nosec G204 -- Fixed executable, isolated kubeconfig and test-owned arguments; no shell.
	out, err := exec.CommandContext(ctx, "kubectl", append([]string{"--kubeconfig", cluster.Kubeconfig, "-n", ns}, args...)...).
		CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("kubectl %v: %w: %s", args, err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func eventually(t *testing.T, description string, check func() (bool, error)) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(
		t.Context(),
		2*time.Second,
		3*time.Minute,
		true,
		func(context.Context) (bool, error) {
			ok, err := check()
			last = err
			return ok && err == nil, nil
		},
	)
	require.NoError(t, err, "%s (last error: %v)", description, last)
}

func ready(p *corev1.Pod) bool {
	if p.DeletionTimestamp != nil {
		return false
	}
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func waitPod(t *testing.T, ns, name string, previous types.UID) *corev1.Pod {
	t.Helper()
	var found *corev1.Pod
	eventually(t, "ready "+name+" pod", func() (bool, error) {
		pods, err := cluster.Clientset.CoreV1().
			Pods(ns).
			List(t.Context(), metav1.ListOptions{LabelSelector: "multigres.com/cluster=" + name})
		if err != nil {
			return false, err
		}
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.UID != previous && ready(p) && hasContainer(p, "minigres") {
				found = p
				return true, nil
			}
		}
		return false, nil
	})
	return found
}

func getPod(t *testing.T, ns, name string) *corev1.Pod {
	t.Helper()
	p, err := cluster.Clientset.CoreV1().Pods(ns).Get(t.Context(), name, metav1.GetOptions{})
	require.NoError(t, err)
	return p
}

func minigresStatus(t *testing.T, p *corev1.Pod) corev1.ContainerStatus {
	t.Helper()
	for _, s := range p.Status.ContainerStatuses {
		if s.Name == "minigres" {
			return s
		}
	}
	t.Fatal("missing Minigres container status")
	return corev1.ContainerStatus{}
}

func dataPVC(t *testing.T, ns string, p *corev1.Pod) *corev1.PersistentVolumeClaim {
	t.Helper()
	for _, v := range p.Spec.Volumes {
		if v.Name == "pgdata" && v.PersistentVolumeClaim != nil {
			pvc, err := cluster.Clientset.CoreV1().
				PersistentVolumeClaims(ns).
				Get(t.Context(), v.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
			require.NoError(t, err)
			return pvc
		}
	}
	t.Fatal("missing pgdata PVC")
	return nil
}

func sql(ns, pod, query string) (string, error) {
	// Arguments go directly to psql; SQL quoting never passes through a shell.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// #nosec G204 -- Fixed executable and test-owned SQL passed as argv, never shell-expanded.
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", cluster.Kubeconfig, "-n", ns, "exec", pod, "-c", "postgres", "--",
		"env", "PGPASSWORD=postgres", "PGCONNECT_TIMEOUT=3", "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
		"host=lite-multigateway port=5432 user=postgres dbname=postgres sslmode=disable", "-c", query).
		CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("SQL: %w: %s", err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func sqlMust(t *testing.T, ns, pod, query, want string) {
	t.Helper()
	got, err := sql(ns, pod, query)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func sqlEventually(t *testing.T, ns, pod, query, want string) {
	t.Helper()
	eventually(
		t,
		"SQL: "+query,
		func() (bool, error) { got, err := sql(ns, pod, query); return got == want, err },
	)
}

func fingerprint(t *testing.T, ns, pod string) string {
	t.Helper()
	got, err := sql(ns, pod, fingerprintQuery)
	require.NoError(t, err)
	return got
}

func scaleEtcd(t *testing.T, ns string, replicas int) {
	t.Helper()
	_, err := kubectl(
		ns,
		"scale",
		"deployment/external-etcd",
		fmt.Sprintf("--replicas=%d", replicas),
	)
	require.NoError(t, err)
}

func etcdAbsent(ns string) (bool, error) {
	pods, err := cluster.Clientset.CoreV1().
		Pods(ns).
		List(context.Background(), metav1.ListOptions{LabelSelector: "app=external-etcd"})
	if err != nil {
		return false, err
	}
	return len(pods.Items) == 0, nil
}

func assertEtcdAbsent(t *testing.T, ns string) {
	t.Helper()
	absent, err := etcdAbsent(ns)
	require.NoError(t, err)
	require.True(t, absent, "etcd must remain stopped throughout recovery")
}

func admin(t *testing.T, ns, pod, mode string) {
	t.Helper()
	// A fresh tunnel per case prevents a failed TLS handshake from poisoning
	// subsequent cases. Negative assertions inspect the actual transport error.
	address, stop := forward(t, ns, pod, 15270)
	defer stop()
	secret, err := cluster.Clientset.CoreV1().
		Secrets(ns).
		Get(t.Context(), "multigres-operator.lite."+ns+".multigres.internal", metav1.GetOptions{})
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(secret.Data["ca.crt"]))
	cert, err := tls.X509KeyPair(secret.Data["tls.crt"], secret.Data["tls.key"])
	require.NoError(t, err)
	config := &tls.Config{
		RootCAs:      roots,
		ServerName:   "multipooler.lite." + ns + ".multigres.internal",
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if mode == "wrong-server" {
		config.ServerName = "wrong.multigres.internal"
	}
	if mode == "no-client-cert" {
		config.Certificates = nil
	}
	transport := credentials.NewTLS(config)
	if mode == "plaintext" {
		transport = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(transport))
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	api := manager.NewMultipoolerManagerClient(conn)
	if mode == "reload" {
		resp, err := api.ReloadConfig(ctx, &data.ReloadConfigRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp.GetConfigLoadTime())
		return
	}
	resp, err := api.Status(ctx, &data.StatusRequest{})
	switch mode {
	case "status":
		require.NoError(t, err)
		require.Equal(t, metadata.PoolerType_PRIMARY, resp.GetStatus().GetPoolerType())
	case "plaintext":
		require.Error(t, err)
		require.Regexp(t, `EOF|connection reset by peer`, err.Error())
	case "no-client-cert":
		require.ErrorContains(t, err, "certificate required")
	case "wrong-server":
		require.ErrorContains(t, err, "certificate is valid for")
	default:
		t.Fatalf("unknown TLS probe %q", mode)
	}
	t.Logf("admin TLS %s passed", mode)
}

func forward(t *testing.T, ns, pod string, port int) (string, func()) {
	t.Helper()
	transport, upgrader, err := spdy.RoundTripperFor(cluster.RestCfg)
	require.NoError(t, err)
	url := cluster.Clientset.CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		Namespace(ns).
		Name(pod).
		SubResource("portforward").
		URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)
	stop, ready := make(chan struct{}), make(chan struct{})
	fw, err := portforward.New(
		dialer,
		[]string{fmt.Sprintf("0:%d", port)},
		stop,
		ready,
		io.Discard,
		io.Discard,
	)
	require.NoError(t, err)
	// Cleanup is installed before waiting so failed startup cannot leak a tunnel.
	cleanup := func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
	}
	t.Cleanup(cleanup)
	errors := make(chan error, 1)
	go func() { errors <- fw.ForwardPorts() }()
	select {
	case <-ready:
	case err := <-errors:
		t.Fatalf("port-forward: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("port-forward timeout")
	}
	ports, err := fw.GetPorts()
	require.NoError(t, err)
	require.Len(t, ports, 1)
	return fmt.Sprintf("127.0.0.1:%d", ports[0].Local), cleanup
}

func topologyRegistered(ns, ip string) (bool, error) {
	out, err := kubectl(
		ns,
		"exec",
		"deployment/external-etcd",
		"--",
		"etcdctl",
		"--endpoints=https://external-etcd."+ns+".svc:2379",
		"--cacert=/tls/ca.crt",
		"--cert=/tls/tls.crt",
		"--key=/tls/tls.key",
		"get",
		"/multigres/"+ns+"/lite/",
		"--prefix",
		"--write-out=json",
	)
	if err != nil {
		return false, err
	}
	var result struct {
		KVs []struct{ Key, Value []byte } `json:"kvs"`
	}
	if err = json.Unmarshal([]byte(out), &result); err != nil {
		return false, err
	}
	var gateway, pooler bool
	for _, kv := range result.KVs {
		key := string(kv.Key)
		if strings.Contains(key, "/gateways/") {
			var record metadata.Multigateway
			if err = proto.Unmarshal(kv.Value, &record); err != nil {
				return false, err
			}
			gateway = record.GetHostname() == ip
		}
		if strings.Contains(key, "/poolers/") {
			var record metadata.Multipooler
			if err = proto.Unmarshal(kv.Value, &record); err != nil {
				return false, err
			}
			pooler = record.GetHostname() == ip
		}
	}
	return gateway && pooler, nil
}

func hasContainer(p *corev1.Pod, name string) bool {
	for _, c := range p.Spec.Containers {
		if c.Name == name {
			return true
		}
	}
	return false
}

func applyManifest(t *testing.T, ns, manifest string) {
	t.Helper()
	// #nosec G204 -- Fixed executable and the dedicated test cluster's kubeconfig.
	cmd := exec.CommandContext(
		t.Context(),
		"kubectl",
		"--kubeconfig",
		cluster.Kubeconfig,
		"-n",
		ns,
		"apply",
		"-f",
		"-",
	)
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

// startLiteRuntime provisions real Minigres and PostgreSQL with admin and topology mTLS.
func startLiteRuntime(
	t *testing.T,
	commonName string,
) (*multigresv1alpha1.MultigresCluster, *corev1.Pod) {
	t.Helper()
	cluster.EnsureTopoTLSIssuer(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	require.NoError(t, err)
	cr := framework.MustLoadCluster("test/e2e/dedicated/minigres/testdata/cluster.yaml", ns)
	cr.Spec.CertCommonName = commonName
	cr.Spec.GlobalTopoServer.External.Endpoints = []multigresv1alpha1.EndpointUrl{
		multigresv1alpha1.EndpointUrl("https://external-etcd." + ns + ".svc:2379"),
	}
	// The fixture pins a known compatible runtime, while the nightly canary's
	// E2E_* overrides (applied by the loader) qualify the actual candidate.
	require.NoError(t, framework.LoadImages(t.Context(), cluster.Name, []string{
		string(cr.Spec.Images.Multipooler), string(cr.Spec.Images.Postgres),
	}))
	t.Logf(
		"runtime images: minigres=%s postgres=%s",
		cr.Spec.Images.Multipooler,
		cr.Spec.Images.Postgres,
	)
	applyManifest(t, ns, strings.ReplaceAll(etcdManifest, "NAMESPACE", ns))
	require.NoError(t, c.Create(t.Context(), cr))
	return cr, waitPod(t, ns, "lite", "")
}
