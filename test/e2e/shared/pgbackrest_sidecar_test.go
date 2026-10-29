//go:build e2e

package shared_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/test/e2e/framework"
)

const (
	// The images are built from the multigres feature branch (make
	// pgbr-e2e-images). The sidecar image is the multipooler image: the operator
	// derives images.pgbackrestd from it, so there is no separate override.
	multigresE2EImage = "multigres:pgbr-e2e"
	pgctldE2EImage    = "pgctld:pgbr-e2e"

	pgbrFixture      = "test/e2e/fixtures/pgbackrest-sidecar/cluster.yaml"
	pgbrClusterName  = "e2e-pgbr"
	pgbrCipherSecret = "e2e-pgbr-cipher" //nolint:gosec // Secret object name, not a credential.

	pgbrDatabase   = "postgres"
	pgbrTableGroup = "default"
	pgbrShard      = "0-inf"

	sidecarName   = shardcontroller.PgBackRestSidecarContainerName
	postgresName  = "postgres"
	repoRoot      = shardcontroller.BackupMountPath
	walSocketPath = shardcontroller.PgBackRestWALSocketPath
)

var cluster *framework.Cluster

func TestMain(m *testing.M) {
	setDefaultEnv("E2E_POSTGRES_IMAGE", pgctldE2EImage)
	for _, name := range []string{
		"E2E_MULTIADMIN_IMAGE", "E2E_MULTIORCH_IMAGE", "E2E_MULTIPOOLER_IMAGE", "E2E_MULTIGATEWAY_IMAGE",
	} {
		setDefaultEnv(name, multigresE2EImage)
	}

	// A dedicated cluster with its own name: the shared one is reused by other
	// packages and must not be torn down or disturbed by this test.
	name := os.Getenv("E2E_PGBR_CLUSTER")
	if name == "" {
		name = fmt.Sprintf("e2e-pgbr-%d", os.Getpid()%100000)
	}
	var err error
	cluster, err = framework.NewDedicatedCluster(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e setup: %v\n", err)
		if cluster != nil {
			cluster.Destroy()
		}
		// NewDedicatedCluster may fail after the Kind cluster exists.
		// #nosec G204 G702 -- Fixed executables; arguments are test-controlled names and scripts, no shell expansion on the host.
		_ = exec.Command("kind", "delete", "cluster", "--name", name).Run()
		os.Exit(1)
	}
	code := m.Run()
	if !framework.ShouldKeepCluster(code != 0) {
		cluster.Destroy()
	}
	os.Exit(code)
}

func setDefaultEnv(name, value string) {
	if os.Getenv(name) == "" {
		_ = os.Setenv(name, value)
	}
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

type pgbr struct {
	ns string
	cr *multigresv1alpha1.MultigresCluster
	c  client.Client
}

// newPgbr creates the namespace, the repository volume, the cipher Secret and
// the MultigresCluster, then waits for a healthy shard.
func newPgbr(t *testing.T, mutate func(*multigresv1alpha1.MultigresCluster)) *pgbr {
	t.Helper()
	ns := cluster.CreateNamespace(t)
	createStaticRWXVolume(t, ns)
	createCipherSecret(t, ns)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}
	cr := framework.MustLoadCluster(pgbrFixture, ns)
	if mutate != nil {
		mutate(cr)
	}
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	p := &pgbr{ns: ns, cr: cr, c: c}
	p.waitSettled(t)
	return p
}

// createCipherSecret supplies the key file the sidecar reads, so it exists on
// the sidecar's mount for the SymlinkDenied case to aim at.
func createCipherSecret(t *testing.T, ns string) {
	t.Helper()
	keys, err := json.Marshal(map[string]string{"1": "e2e-pgbr-passphrase"})
	if err != nil {
		t.Fatalf("marshal cipher keys: %v", err)
	}
	_, err = cluster.Clientset.CoreV1().Secrets(ns).Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: pgbrCipherSecret},
		Data:       map[string][]byte{shardcontroller.PgBackRestCipherKeyDataKey: keys},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create cipher secret: %v", err)
	}
}

// createStaticRWXVolume supplies the claim for the shared backup repository. A
// Kind cluster here has one node, so a hostPath is enough for every pooler to
// mount the same RWX claim. This repeats the helper in the verification
// package: both are e2e-suite scaffolding for something the framework does not
// provide (a provisioner for RWX), not a testkit gap, and the task's file
// scope kept it from moving into the framework.
func createStaticRWXVolume(t *testing.T, ns string) {
	t.Helper()
	name := "e2e-rwx-" + ns
	hostPath := "/var/local/multigres-e2e-rwx/" + ns
	node := cluster.Name + "-control-plane"
	for _, args := range [][]string{{"mkdir", "-p", hostPath}, {"chmod", "0777", hostPath}} {
		// #nosec G204 -- Fixed executables; arguments are test-controlled names and scripts, no shell expansion on the host.
		out, err := exec.Command("docker", append([]string{"exec", node}, args...)...).
			CombinedOutput()
		if err != nil {
			t.Fatalf("%s static RWX hostPath: %v\n%s", args[0], err, out)
		}
	}
	_, err := cluster.Clientset.CoreV1().
		PersistentVolumes().
		Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: apiresource.MustParse("1Gi"),
				},
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteMany,
				},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				StorageClassName:              "e2e-rwx",
				NodeAffinity: &corev1.VolumeNodeAffinity{
					Required: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: []corev1.NodeSelectorRequirement{{
								Key:      "node-role.kubernetes.io/control-plane",
								Operator: corev1.NodeSelectorOpExists,
							}},
						}},
					},
				},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: hostPath,
						Type: ptr.To(corev1.HostPathDirectoryOrCreate),
					},
				},
			},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create static RWX volume: %v", err)
	}
	t.Cleanup(func() {
		_ = cluster.Clientset.CoreV1().PersistentVolumes().Delete(
			context.Background(), name, metav1.DeleteOptions{})
		// #nosec G204 -- Fixed executables; arguments are test-controlled names and scripts, no shell expansion on the host.
		_ = exec.Command("docker", "exec", node, "rm", "-rf", hostPath).Run()
	})
}

// ---------------------------------------------------------------------------
// Observation helpers
// ---------------------------------------------------------------------------

func (p *pgbr) shard(ctx context.Context) (*multigresv1alpha1.Shard, error) {
	shards := &multigresv1alpha1.ShardList{}
	if err := p.c.List(ctx, shards,
		client.InNamespace(p.ns),
		client.MatchingLabels{metadata.LabelMultigresCluster: p.cr.Name},
	); err != nil {
		return nil, err
	}
	if len(shards.Items) != 1 {
		return nil, fmt.Errorf("shard count = %d, want 1", len(shards.Items))
	}
	return &shards.Items[0], nil
}

func (p *pgbr) poolPods(ctx context.Context) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := p.c.List(ctx, pods,
		client.InNamespace(p.ns),
		client.MatchingLabels{
			metadata.LabelMultigresCluster: p.cr.Name,
			metadata.LabelMultigresPool:    "default",
		},
	); err != nil {
		return nil, err
	}
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	return pods.Items, nil
}

func hasInitContainer(pod *corev1.Pod, name string) bool {
	for _, c := range pod.Spec.InitContainers {
		if c.Name == name {
			return true
		}
	}
	return false
}

func podReady(pod *corev1.Pod) bool {
	if !pod.DeletionTimestamp.IsZero() {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// waitSettled waits until every pool pod is ready, runs the sidecar, no
// pod is draining, and the shard is healthy with a primary.
func (p *pgbr) waitSettled(t *testing.T) {
	t.Helper()
	p.waitSettledWithin(t, 15*time.Minute)
}

func (p *pgbr) waitSettledWithin(t *testing.T, timeout time.Duration) {
	t.Helper()
	if ok, last := p.trySettled(t, timeout); !ok {
		p.dump(t)
		t.Fatalf("timed out waiting for a settled shard: %s", last)
	}
}

// trySettled reports whether the shard settled within timeout, and the last
// reason it had not when it did not.
func (p *pgbr) trySettled(t *testing.T, timeout time.Duration) (bool, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var last string
	err := wait.PollUntilContextCancel(
		ctx,
		3*time.Second,
		true,
		func(context.Context) (bool, error) {
			// Own deadline per call: the poll context ending must not turn the last
			// real observation into a rate-limiter error.
			cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer ccancel()
			shard, err := p.shard(cctx)
			if err != nil {
				last = err.Error()
				return false, nil
			}
			pods, err := p.poolPods(cctx)
			if err != nil || len(pods) != 3 {
				last = fmt.Sprintf("pool pods = %d (%v)", len(pods), err)
				return false, nil
			}
			for i := range pods {
				pod := &pods[i]
				if !hasInitContainer(pod, sidecarName) {
					last = fmt.Sprintf("pod %s has no sidecar", pod.Name)
					return false, nil
				}
				if !podReady(pod) || pod.Annotations[metadata.AnnotationDrainState] != "" {
					last = fmt.Sprintf("pod %s not ready or draining", pod.Name)
					return false, nil
				}
			}
			if shard.Status.Phase != multigresv1alpha1.PhaseHealthy || !shard.Status.OrchReady {
				last = fmt.Sprintf(
					"shard phase %q, orchReady %v",
					shard.Status.Phase,
					shard.Status.OrchReady,
				)
				return false, nil
			}
			if _, ok := primaryName(shard); !ok {
				last = "no primary"
				return false, nil
			}
			return true, nil
		},
	)
	if err != nil {
		return false, fmt.Sprintf("%v (last: %s)", err, last)
	}
	return true, ""
}

func primaryName(shard *multigresv1alpha1.Shard) (string, bool) {
	for name, role := range shard.Status.PodRoles {
		if role == "PRIMARY" {
			return name, true
		}
	}
	return "", false
}

func (p *pgbr) primaryAndReplicas(t *testing.T) (primary *corev1.Pod, replicas []*corev1.Pod) {
	t.Helper()
	ctx := context.Background()
	shard, err := p.shard(ctx)
	if err != nil {
		t.Fatalf("get shard: %v", err)
	}
	pods, err := p.poolPods(ctx)
	if err != nil {
		t.Fatalf("list pool pods: %v", err)
	}
	for i := range pods {
		pod := &pods[i]
		switch shard.Status.PodRoles[pod.Name] {
		case "PRIMARY":
			primary = pod
		case "REPLICA":
			replicas = append(replicas, pod)
		}
	}
	if primary == nil || len(replicas) == 0 {
		t.Fatalf("roles %v: want a primary and replicas", shard.Status.PodRoles)
	}
	return primary, replicas
}

func (p *pgbr) dump(t *testing.T) {
	t.Helper()
	if shard, err := p.shard(context.Background()); err == nil {
		t.Logf("shard phase=%q orchReady=%v message=%q roles=%v", shard.Status.Phase,
			shard.Status.OrchReady, shard.Status.Message, shard.Status.PodRoles)
	}
	preset := testutil.DefaultOperatorPreset()
	// #nosec G204 G702 -- Fixed executable; arguments are test-controlled names.
	if out, err := exec.Command("kubectl", "--kubeconfig", cluster.Kubeconfig, "-n", preset.Namespace,
		"logs", "deploy/"+preset.DeploymentName, "--tail=3000").
		CombinedOutput(); err == nil {
		var keep []string
		for _, l := range strings.Split(string(out), "\n") {
			if strings.Contains(l, p.ns) && !strings.Contains(l, "DEBUG") &&
				!strings.Contains(l, "bootstrapping PKI") {
				keep = append(keep, l)
			}
		}
		if len(keep) > 40 {
			keep = keep[len(keep)-40:]
		}
		t.Logf(
			"operator log (this namespace, last %d non-debug lines):\n%s",
			len(keep),
			strings.Join(keep, "\n"),
		)
	}
	pods, err := p.poolPods(context.Background())
	if err != nil {
		t.Logf("dump: list pods: %v", err)
		return
	}
	for _, pod := range pods {
		t.Logf(
			"pod %s phase=%s ready=%v drain=%q sidecar=%v",
			pod.Name,
			pod.Status.Phase,
			podReady(
				&pod,
			),
			pod.Annotations[metadata.AnnotationDrainState],
			hasInitContainer(&pod, sidecarName),
		)
		for _, cs := range pod.Status.InitContainerStatuses {
			t.Logf(
				"  init %s ready=%v started=%v restarts=%d state=%+v last=%+v",
				cs.Name,
				cs.Ready,
				ptr.Deref(cs.Started, false),
				cs.RestartCount,
				cs.State,
				cs.LastTerminationState,
			)
		}
	}
}

// ---------------------------------------------------------------------------
// In-pod commands
// ---------------------------------------------------------------------------

// kexec runs a shell script in a container of a pod and returns its combined
// output. A non-zero exit is returned as an error carrying the output.
func kexec(pod *corev1.Pod, container, script string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// #nosec G204 -- Fixed executables; arguments are test-controlled names and scripts, no shell expansion on the host.
	cmd := exec.CommandContext(ctx, "kubectl", "--kubeconfig", cluster.Kubeconfig,
		"exec", "-n", pod.Namespace, pod.Name, "-c", container, "--", "bash", "-c", script)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err != nil {
		return out.String(), fmt.Errorf(
			"exec in %s/%s: %w: %s",
			pod.Name,
			container,
			err,
			strings.TrimSpace(out.String()),
		)
	}
	return out.String(), nil
}

func mustExec(t *testing.T, pod *corev1.Pod, container, script string) string {
	t.Helper()
	out, err := kexec(pod, container, script)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return out
}

func containerLogs(t *testing.T, pod *corev1.Pod, container string) string {
	t.Helper()
	// #nosec G204 -- Fixed executables; arguments are test-controlled names and scripts, no shell expansion on the host.
	out, err := exec.Command("kubectl", "--kubeconfig", cluster.Kubeconfig,
		"logs", "-n", pod.Namespace, pod.Name, "-c", container, "--tail=-1").CombinedOutput()
	if err != nil {
		t.Fatalf("logs %s/%s: %v: %s", pod.Name, container, err, out)
	}
	return string(out)
}

// repoFiles lists every file in the filesystem repository, as seen from the
// sidecar, which is the one container that mounts it.
func repoFiles(t *testing.T, pod *corev1.Pod) []string {
	t.Helper()
	out := mustExec(t, pod, sidecarName, "find "+repoRoot+" -type f | sort")
	return strings.Fields(out)
}

func diffFiles(before, after []string) []string {
	seen := make(map[string]struct{}, len(before))
	for _, f := range before {
		seen[f] = struct{}{}
	}
	var added []string
	for _, f := range after {
		if _, ok := seen[f]; !ok {
			added = append(added, f)
		}
	}
	return added
}

// ---------------------------------------------------------------------------
// Backups through multiadmin
// ---------------------------------------------------------------------------

func (p *pgbr) multiadminCurl(t *testing.T, pod *corev1.Pod, method, path, body string) string {
	t.Helper()
	url := fmt.Sprintf("http://%s-multiadmin:18000%s", p.cr.Name, path)
	script := fmt.Sprintf(
		"curl -sS --max-time 60 -X %s -H 'Content-Type: application/json' ",
		method,
	)
	if body != "" {
		script += fmt.Sprintf("-d '%s' ", body)
	}
	script += "'" + url + "'"
	return mustExec(t, pod, postgresName, script)
}

// runBackup starts a full backup through multiadmin and waits for the job to
// finish. It returns the job outcome without failing, so callers can assert
// that a backup fails as well as that it succeeds.
func (p *pgbr) runBackup(
	t *testing.T,
	via *corev1.Pod,
	forcePrimary bool,
) (backupID, failure string) {
	t.Helper()
	body := fmt.Sprintf(
		`{"database":%q,"tableGroup":%q,"shard":%q,"type":"full","forcePrimary":%v}`,
		pgbrDatabase,
		pgbrTableGroup,
		pgbrShard,
		forcePrimary,
	)
	var start struct {
		JobID string `json:"jobId"`
	}
	raw := p.multiadminCurl(t, via, "POST", "/api/v1/backups", body)
	if err := json.Unmarshal([]byte(raw), &start); err != nil || start.JobID == "" {
		return "", fmt.Sprintf("start backup: %q (%v)", raw, err)
	}
	var final struct {
		Status       string `json:"status"`
		ErrorMessage string `json:"errorMessage"`
		BackupID     string `json:"backupId"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(
		ctx,
		3*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			raw := p.multiadminCurl(t, via, "GET", fmt.Sprintf(
				"/api/v1/jobs/%s?database=%s&table_group=%s&shard=%s",
				start.JobID, pgbrDatabase, pgbrTableGroup, pgbrShard), "")
			final.Status, final.ErrorMessage, final.BackupID = "", "", ""
			if err := json.Unmarshal([]byte(raw), &final); err != nil {
				return false, nil
			}
			return strings.HasSuffix(final.Status, "COMPLETED") ||
				strings.HasSuffix(final.Status, "FAILED"), nil
		},
	)
	if err != nil {
		return "", fmt.Sprintf(
			"job %s did not finish: %v (last status %q)",
			start.JobID,
			err,
			final.Status,
		)
	}
	if strings.HasSuffix(final.Status, "FAILED") {
		return "", fmt.Sprintf("job %s failed: %s", start.JobID, final.ErrorMessage)
	}
	return final.BackupID, ""
}

// mustBackup runs a backup and fails the test if it does not succeed within
// four minutes. Right after a roll or failover, a replica that has not yet been
// told of its primary answers "primary host/port not configured", and
// multiadmin may still dial a pooler address that no longer exists; both settle
// on their own, so only a failure that persists counts.
func (p *pgbr) mustBackup(t *testing.T, forcePrimary bool) string {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for {
		primary, replicas := p.primaryAndReplicas(t)
		via := primary
		if len(replicas) > 0 {
			via = replicas[0]
		}
		id, failure := p.runBackup(t, via, forcePrimary)
		if failure == "" {
			return id
		}
		if time.Now().After(deadline) {
			t.Fatalf("backup still failing after 4m: %s", failure)
		}
		t.Logf("DEGRADED: backup attempt failed, retrying: %s", firstLines(failure, 1))
		time.Sleep(5 * time.Second)
	}
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

func TestPgBackRestSidecar(t *testing.T) {
	p := newPgbr(t, nil)

	// Order matters: the cases share one cluster, and the last two replace its
	// pods.
	t.Run("Contract", p.testContract)
	t.Run("StandbyBackup", p.testStandbyBackup)
	t.Run("Confinement", p.testConfinement)
	t.Run("NoPgNetReach", p.testNoPgNetReach)
	t.Run("SymlinkDenied", p.testSymlinkDenied)
	t.Run("DrainArchivesLastSegment", p.testDrainArchivesLastSegment)
	t.Run("StuckPodEscape", p.testStuckPodEscape)
}

func findInit(pod *corev1.Pod, name string) *corev1.Container {
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == name {
			return &pod.Spec.InitContainers[i]
		}
	}
	return nil
}

// testContract checks the isolation the sidecar exists for, on a live pod built
// by the production builders: postgres holds no credentials and cannot reach
// the control socket, and the two containers share an identity but not a
// process namespace.
func (p *pgbr) testContract(t *testing.T) {
	primary, _ := p.primaryAndReplicas(t)
	pg := findInit(primary, postgresName)
	sc := findInit(primary, sidecarName)
	if pg == nil || sc == nil {
		t.Fatalf(
			"pod %s: postgres=%v sidecar=%v init containers, want both",
			primary.Name,
			pg != nil,
			sc != nil,
		)
	}

	for _, e := range pg.Env {
		if strings.HasPrefix(e.Name, "AWS_") {
			t.Errorf("postgres container env %s: want no AWS_* variable", e.Name)
		}
	}
	forbidden := map[string]bool{
		shardcontroller.PgBackRestControlVolumeName:   true,
		shardcontroller.PgBackRestS3VolumeName:        true,
		shardcontroller.PgBackRestCipherKeyVolumeName: true,
		shardcontroller.PgBackRestCertVolumeName:      true,
		shardcontroller.BackupVolumeName:              true,
		shardcontroller.PgBackRestStateVolumeName:     true,
	}
	for _, m := range pg.VolumeMounts {
		if forbidden[m.Name] {
			t.Errorf(
				"postgres container mounts %q (at %s), which only the sidecar or multipooler may see",
				m.Name,
				m.MountPath,
			)
		}
	}
	if sc.SecurityContext == nil || pg.SecurityContext == nil ||
		sc.SecurityContext.RunAsUser == nil || pg.SecurityContext.RunAsUser == nil ||
		*sc.SecurityContext.RunAsUser != *pg.SecurityContext.RunAsUser {
		t.Errorf(
			"sidecar runAsUser %v != postgres runAsUser %v",
			sc.SecurityContext,
			pg.SecurityContext,
		)
	}
	if sc.SecurityContext != nil {
		if sp := sc.SecurityContext.SeccompProfile; sp == nil ||
			sp.Type != corev1.SeccompProfileTypeRuntimeDefault {
			t.Errorf("sidecar seccompProfile = %v, want RuntimeDefault", sp)
		}
		if ro := sc.SecurityContext.ReadOnlyRootFilesystem; ro == nil || !*ro {
			t.Errorf("sidecar readOnlyRootFilesystem = %v, want true", ro)
		}
	}
	if psc := primary.Spec.SecurityContext; psc == nil || psc.FSGroup == nil ||
		*psc.FSGroup != shardcontroller.DefaultPostgresGID {
		t.Errorf("pod securityContext fsGroup = %v, want the postgres gid %d",
			psc, shardcontroller.DefaultPostgresGID)
	}
	if share := primary.Spec.ShareProcessNamespace; share != nil && *share {
		t.Errorf("shareProcessNamespace = true, want false or unset")
	}

	// The live view: what the running postgres container can actually see.
	out := mustExec(t, primary, postgresName, `env | grep '^AWS_' || true`)
	if strings.TrimSpace(out) != "" {
		t.Errorf("postgres container environment has AWS_* variables: %q", out)
	}
	out = mustExec(
		t,
		primary,
		postgresName,
		`for p in /run/pgbackrest-control /secrets /certs /backups /var/lib/pgbackrestd; do [ -e $p ] && echo "$p"; done; true`,
	)
	if strings.TrimSpace(out) != "" {
		t.Errorf("postgres container can see sidecar-only paths: %q", out)
	}
	pgUID := strings.TrimSpace(mustExec(t, primary, postgresName, "id -u"))
	scUID := strings.TrimSpace(mustExec(t, primary, sidecarName, "id -u"))
	if pgUID != scUID {
		t.Errorf("live uid: postgres %s, sidecar %s", pgUID, scUID)
	}
	// Separate PID namespaces: postgres must not see the sidecar's process.
	out = mustExec(t, primary, postgresName, `ps -eo args | grep -c '[p]gbackrestd' || true`)
	if strings.TrimSpace(out) != "0" {
		t.Errorf(
			"postgres container sees %s pgbackrestd process(es); want its own PID namespace",
			strings.TrimSpace(out),
		)
	}
}

// landlockProbePolicy grants read and exec on /usr and read on /etc only, the
// same wrapper the sidecar uses for every pgBackRest child.
const landlockProbePolicy = `{"Rules":[{"Path":"/etc","Access":1},{"Path":"/usr","Access":5}]}`

// landlockProbe runs two commands under the sandbox wrapper in a container
// that carries the sidecar's image and identity: one that the policy allows,
// and one that reads a file outside it. It reports whether confinement is
// really enforced rather than inferred from a flag.
func landlockProbe(t *testing.T, pod *corev1.Pod, container, denyPath string) {
	t.Helper()
	script := fmt.Sprintf(`P=$(printf '%%s' '%s' | base64 -w0)
/multigres/bin/pgbackrestd sandbox-exec --policy "$P" -- /usr/bin/cat /etc/hostname >/dev/null && echo ALLOWED_OK
/multigres/bin/pgbackrestd sandbox-exec --policy "$P" -- /usr/bin/cat %s 2>&1; echo "denied_exit=$?"`,
		landlockProbePolicy, denyPath)
	out := mustExec(t, pod, container, script)
	if !strings.Contains(out, "ALLOWED_OK") {
		t.Errorf("sandboxed read inside the policy failed: %q", out)
	}
	if !strings.Contains(out, "denied_exit=1") ||
		!strings.Contains(strings.ToLower(out), "permission denied") {
		t.Errorf(
			"sandboxed read of %s outside the policy was not denied with EACCES: %q",
			denyPath,
			out,
		)
	}
}

// testConfinement records and checks, on the kind nodes' kernel, the three
// facts the sidecar's design leans on: Landlock confinement is really on, the
// sidecar runs under the container runtime's default seccomp profile, and a
// read-only root filesystem is viable.
func (p *pgbr) testConfinement(t *testing.T) {
	primary, _ := p.primaryAndReplicas(t)

	// #nosec G204 G702 -- Fixed executables; arguments are test-controlled names, no shell expansion on the host.
	node, err := exec.Command("docker", "exec", cluster.Name+"-control-plane", "uname", "-r").
		Output()
	if err != nil {
		t.Fatalf("node kernel: %v", err)
	}
	t.Logf("kind node kernel: %s", strings.TrimSpace(string(node)))

	// 1. Landlock in the live sidecar.
	status := mustExec(t, primary, sidecarName, `grep -E '^(Seccomp|NoNewPrivs):' /proc/1/status`)
	t.Logf("live sidecar /proc/1/status: %s", strings.Join(strings.Fields(status), " "))
	landlockProbe(t, primary, sidecarName, "/var/lib/pgbackrestd/pgpass")

	// The running sidecar wraps each pgBackRest child the same way and has
	// already used it: Configure ran pgBackRest and the stanza exists in the
	// repository, and pgbackrestd refuses to start without Landlock, so a
	// sidecar that could not confine would not be serving.
	if files := repoFiles(t, primary); len(files) == 0 {
		t.Errorf("repository is empty: the sandboxed sidecar has stored nothing")
	}

	// 2 and 3. A probe pod with the sidecar's identity and image under the
	// runtime default seccomp profile, and with a read-only root filesystem.
	probe := p.confinementProbePod(primary)
	if err := p.c.Create(context.Background(), probe); err != nil {
		t.Fatalf("create confinement probe: %v", err)
	}
	t.Cleanup(func() { _ = p.c.Delete(context.Background(), probe) })
	p.waitPodReady(t, probe.Name, 4*time.Minute)
	live := &corev1.Pod{}
	if err := p.c.Get(context.Background(), client.ObjectKeyFromObject(probe), live); err != nil {
		t.Fatalf("get probe: %v", err)
	}
	status = mustExec(t, live, "probe", `grep -E '^Seccomp:' /proc/1/status`)
	t.Logf("probe pod (seccompProfile RuntimeDefault, readOnlyRootFilesystem) /proc/1/status: %s",
		strings.Join(strings.Fields(status), " "))
	if !strings.Contains(status, "2") {
		t.Errorf("probe pod seccomp mode = %q, want 2 (filter)", strings.TrimSpace(status))
	}
	landlockProbe(t, live, "probe", "/var/lib/pgbackrestd/pgpass")
	// Reaching ready means the real sidecar passed its startup probe with
	// every write confined to an emptyDir.
	out := mustExec(t, live, "probe", "/multigres/bin/pgbackrestd health && echo HEALTHY")
	if !strings.Contains(out, "HEALTHY") {
		t.Errorf("sidecar under readOnlyRootFilesystem is not healthy: %q", out)
	}
}

// confinementProbePod runs the sidecar itself, as serve, in a pod with the
// default seccomp profile and a read-only root filesystem. Every directory it
// writes is an emptyDir, which is also how the production pod provides them.
func (p *pgbr) confinementProbePod(src *corev1.Pod) *corev1.Pod {
	sc := findInit(src, sidecarName)
	secCtx := sc.SecurityContext.DeepCopy()
	secCtx.ReadOnlyRootFilesystem = ptr.To(true)
	secCtx.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}

	mounts := []corev1.VolumeMount{}
	vols := []corev1.Volume{}
	for _, m := range []struct{ name, path string }{
		{"data", "/var/lib/pooler"},
		{"wal", shardcontroller.PgBackRestWALMountPath},
		{"control", shardcontroller.PgBackRestControlMountPath},
		{"state", shardcontroller.PgBackRestStateMountPath},
		{"repo", repoRoot},
	} {
		mounts = append(mounts, corev1.VolumeMount{Name: m.name, MountPath: m.path})
		vols = append(vols, corev1.Volume{
			Name:         m.name,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "confinement-probe", Namespace: p.ns},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: ptr.To(false),
			Volumes:                      vols,
			Containers: []corev1.Container{{
				Name:  "probe",
				Image: sc.Image,
				Command: []string{
					"tini", "--", shardcontroller.PgBackRestSidecarBinary, "serve",
				},
				Args: []string{
					"--data-volume=/var/lib/pooler",
					"--pgdata=/var/lib/pooler/pg_data",
					"--pg-socket-dir=/var/lib/pooler/pg_sockets",
					"--pg-port=5432",
					"--pg-user=postgres",
					"--pg-password-file=/etc/hostname",
					"--location-type=filesystem",
					"--fs-repo-path=" + repoRoot,
					"--tls-cert-dir=/certs/pgbackrest",
					"--restore-pidfile=/var/lib/pooler/restore_command.pid",
				},
				SecurityContext: secCtx,
				VolumeMounts:    mounts,
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{
						Command: []string{shardcontroller.PgBackRestSidecarBinary, "health"},
					}},
					PeriodSeconds: 3,
				},
			}},
		},
	}
}

func (p *pgbr) waitPodReady(t *testing.T, name string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := wait.PollUntilContextCancel(
		ctx,
		2*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			pod := &corev1.Pod{}
			if err := p.c.Get(ctx, client.ObjectKey{Namespace: p.ns, Name: name}, pod); err != nil {
				return false, nil
			}
			return podReady(pod), nil
		},
	)
	if err != nil {
		// #nosec G204 G702 -- Fixed executables; arguments are test-controlled names, no shell expansion on the host.
		out, _ := exec.Command("kubectl", "--kubeconfig", cluster.Kubeconfig, "-n", p.ns,
			"logs", name, "--tail=30").CombinedOutput()
		t.Fatalf("pod %s did not become ready: %v\n%s", name, err, out)
	}
}

// testNoPgNetReach checks that the only TCP port the sidecar's processes
// listen on is 8432, which the loopback is not an authentication boundary for
// (a pg_net request from postgres can reach it), so TLS with a client cert
// must be what stops it.
func (p *pgbr) testNoPgNetReach(t *testing.T) {
	primary, _ := p.primaryAndReplicas(t)

	// /proc/net/tcp is the pod's network namespace, so attribute each
	// listening socket to a process of this container through its fds: the
	// sidecar has its own PID namespace.
	script := `inodes=$(for f in /proc/[0-9]*/fd/*; do readlink "$f" 2>/dev/null; done | sed -n 's/^socket:\[\([0-9]*\)\]$/\1/p' | sort -u)
for f in /proc/net/tcp /proc/net/tcp6; do
  awk -v inodes="$inodes" 'BEGIN{n=split(inodes,a,"\n"); for(i=1;i<=n;i++) have[a[i]]=1}
    NR>1 && $4=="0A" && ($10 in have) {split($2,l,":"); print l[2]}' "$f"
done`
	out := mustExec(t, primary, sidecarName, script)
	ports := strings.Fields(out)
	if len(ports) == 0 {
		t.Fatalf(
			"sidecar listens on no TCP socket; want port 8432 (%x)",
			shardcontroller.PgBackRestPort,
		)
	}
	want := fmt.Sprintf("%04X", shardcontroller.PgBackRestPort)
	for _, port := range ports {
		if !strings.EqualFold(port, want) {
			t.Errorf("sidecar listens on TCP port 0x%s, want only 0x%s", port, want)
		}
	}

	if out := strings.TrimSpace(psqlOn(t, primary,
		`select count(*) from pg_available_extensions where name = 'pg_net'`)); out != "1" {
		t.Skipf("pg_net is not in the fixture image (pgctld:pgbr-e2e is stock postgres 17.7); "+
			"the TCP listener check above stands, the net.http_get half is not run (available=%q)", out)
	}
	psqlOn(t, primary, `create extension if not exists pg_net`)
	psqlOn(t, primary, `select net.http_get('http://127.0.0.1:8432/')`)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		res := psqlOn(
			t,
			primary,
			`select status_code is null and error_msg is not null from net._http_response order by id desc limit 1`,
		)
		if strings.TrimSpace(res) == "t" {
			return
		}
		if strings.TrimSpace(res) == "f" {
			t.Fatalf(
				"net.http_get to the 8432 server succeeded or returned a response; TLS with a client cert should refuse it",
			)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("no pg_net response row recorded for http://127.0.0.1:8432/")
}

// psqlOn runs one statement as the superuser over the pod's unix socket.
func psqlOn(t *testing.T, pod *corev1.Pod, sql string) string {
	t.Helper()
	return mustExec(t, pod, postgresName, fmt.Sprintf(
		`PGPASSWORD=postgres psql -h /var/lib/pooler/pg_sockets -U postgres -d postgres -Atq -c %q`,
		sql,
	))
}

// testSymlinkDenied plants symlinks to the sidecar's cipher key in postgres's
// data directory, the one place postgres controls, and checks that the sidecar
// neither archives nor backs up what they point at.
func (p *pgbr) testSymlinkDenied(t *testing.T) {
	primary, _ := p.primaryAndReplicas(t)
	key := shardcontroller.PgBackRestCipherKeyFilePath
	pgdata := shardcontroller.PgDataPath
	before := repoFiles(t, primary)

	t.Cleanup(func() {
		_, _ = kexec(primary, postgresName, fmt.Sprintf(
			"rm -f %[1]s/pg_wal/00000099.history %[1]s/pg_wal/archive_status/00000099.history.ready %[1]s/base/e2e-symlink",
			pgdata,
		))
	})

	// The archive half. The .ready marker is what makes the async archive-push
	// worker pick the file up, as postgres's own archiver would have done.
	mustExec(t, primary, postgresName, fmt.Sprintf(
		"ln -s %s %s/pg_wal/00000099.history && touch %s/pg_wal/archive_status/00000099.history.ready",
		key,
		pgdata,
		pgdata,
	))
	out, err := kexec(primary, postgresName, fmt.Sprintf(
		"curl -sS --fail-with-body --unix-socket %s --connect-timeout 5 --max-time 120 -X POST http://pgbackrest/wal/00000099.history",
		walSocketPath,
	))
	if err == nil {
		t.Fatalf("archiving a symlink to the cipher key through the WAL socket succeeded: %q", out)
	}
	// pgBackRest's own error comes back in the response body; the sidecar's log
	// may carry it as well, so accept either place.
	evidence := strings.ToLower(out + containerLogs(t, primary, sidecarName))
	if !strings.Contains(evidence, "eacces") && !strings.Contains(evidence, "permission denied") {
		t.Errorf(
			"neither the push response nor the sidecar log shows EACCES or 'permission denied': %s",
			out,
		)
	} else {
		t.Logf("archive push refused: %s", firstLines(grepLines(out, "denied", "EACCES"), 3))
	}
	mustExec(t, primary, postgresName, fmt.Sprintf(
		"rm -f %[1]s/pg_wal/00000099.history %[1]s/pg_wal/archive_status/00000099.history.ready",
		pgdata,
	))

	// The backup half.
	mustExec(t, primary, postgresName, fmt.Sprintf("ln -s %s %s/base/e2e-symlink", key, pgdata))
	if _, failure := p.runBackup(t, primary, true); failure == "" {
		t.Errorf("a backup with a symlink to the cipher key in base/ succeeded")
	} else {
		t.Logf("backup refused: %s", firstLines(failure, 6))
	}

	// Nothing of either attempt may be in the repository. Ordinary archiving of
	// real WAL continues, so look for the planted name and for any new backup.
	for _, f := range diffFiles(before, repoFiles(t, primary)) {
		if strings.Contains(f, "00000099") || strings.HasPrefix(f, repoRoot+"/backup/") {
			t.Errorf("new repository file after the denied attempts: %s", f)
		}
	}
}

func grepLines(s string, needles ...string) string {
	var keep []string
	for _, l := range strings.Split(s, "\n") {
		for _, n := range needles {
			if strings.Contains(strings.ToLower(l), strings.ToLower(n)) {
				keep = append(keep, strings.TrimSpace(l))
				break
			}
		}
	}
	return strings.Join(keep, "\n")
}

func firstLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, " | ")
}

// segmentRe splits a repository archive file name into its WAL segment.
var segmentRe = regexp.MustCompile(`/([0-9A-F]{24})(\.partial)?-[0-9a-f]{40}`)

func archivedSegments(files []string) map[string]bool {
	out := map[string]bool{}
	for _, f := range files {
		if m := segmentRe.FindStringSubmatch(f); m != nil {
			out[m[1]] = true
		}
	}
	return out
}

// testDrainArchivesLastSegment deletes the primary pod and checks that the WAL
// segment it was writing is in the repository by the time the pod is gone: the
// sidecar, being started first, stops last and flushes what postgres left.
func (p *pgbr) testDrainArchivesLastSegment(t *testing.T) {
	primary, replicas := p.primaryAndReplicas(t)
	psqlOn(
		t,
		primary,
		`create table if not exists e2e_drain(i int); insert into e2e_drain select generate_series(1, 5000)`,
	)
	segment := strings.TrimSpace(psqlOn(t, primary, `select pg_walfile_name(pg_current_wal_lsn())`))
	t.Logf("primary %s is writing segment %s", primary.Name, segment)
	if !regexp.MustCompile(`^[0-9A-F]{24}$`).MatchString(segment) {
		t.Fatalf("unexpected segment name %q", segment)
	}
	observer := replicas[0]
	t.Logf(
		"segment archived before the delete: %v",
		archivedSegments(repoFiles(t, observer))[segment],
	)

	uid := primary.UID
	if err := p.c.Delete(context.Background(), primary); err != nil {
		t.Fatalf("delete primary: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(
		ctx,
		2*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			pods, err := p.poolPods(ctx)
			if err != nil {
				return false, nil
			}
			for i := range pods {
				if pods[i].UID == uid {
					return false, nil
				}
			}
			return true, nil
		},
	)
	if err != nil {
		t.Fatalf("old primary pod %s did not go away: %v", primary.Name, err)
	}
	// The repository is on a shared volume, so a surviving pod's sidecar shows
	// what the deleted pod managed to store.
	if !archivedSegments(repoFiles(t, observer))[segment] {
		t.Errorf(
			"segment %s (or its .partial) is not in the repository after the primary pod was deleted",
			segment,
		)
	}
	p.waitSettled(t)
}

// testStandbyBackup checks that a standby backup, which goes through the
// 8432 server of the primary's sidecar, succeeds on the shared repository.
func (p *pgbr) testStandbyBackup(t *testing.T) {
	t.Logf("standby backup %s", p.mustBackup(t, false))
	p.waitSettled(t)
}

// bumpSidecarResources raises the sidecar's CPU request by 1m. The change
// drifts every pool pod's spec hash without pulling an image, so every pod is
// stale against the new spec.
func (p *pgbr) bumpSidecarResources(t *testing.T) {
	t.Helper()
	live := &multigresv1alpha1.MultigresCluster{}
	if err := p.c.Get(context.Background(), client.ObjectKeyFromObject(p.cr), live); err != nil {
		t.Fatalf("get MultigresCluster: %v", err)
	}
	before := live.DeepCopy()
	pool := live.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"]
	cpu := pool.PgBackRestResources.Requests[corev1.ResourceCPU]
	if cpu.IsZero() {
		cpu = apiresource.MustParse("100m")
	}
	cpu.Add(apiresource.MustParse("1m"))
	if pool.PgBackRestResources.Requests == nil {
		pool.PgBackRestResources.Requests = corev1.ResourceList{}
	}
	pool.PgBackRestResources.Requests[corev1.ResourceCPU] = cpu
	live.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"] = pool
	patch, err := client.MergeFrom(before).Data(live)
	if err != nil {
		t.Fatalf("build patch: %v", err)
	}
	if err := p.c.Patch(
		context.Background(),
		p.cr,
		client.RawPatch(types.MergePatchType, patch),
	); err != nil {
		t.Fatalf("patch MultigresCluster: %v", err)
	}
}

// ---------------------------------------------------------------------------
// StuckPodEscape
// ---------------------------------------------------------------------------

func (p *pgbr) scaleOperator(t *testing.T, replicas int32) {
	t.Helper()
	preset := testutil.DefaultOperatorPreset()
	ctx := context.Background()
	scale, err := cluster.Clientset.AppsV1().
		Deployments(preset.Namespace).
		GetScale(ctx, preset.DeploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get operator scale: %v", err)
	}
	scale.Spec.Replicas = replicas
	if _, err := cluster.Clientset.AppsV1().
		Deployments(preset.Namespace).
		UpdateScale(ctx, preset.DeploymentName, scale, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("scale operator to %d: %v", replicas, err)
	}
	err = wait.PollUntilContextTimeout(
		ctx,
		2*time.Second,
		3*time.Minute,
		true,
		func(ctx context.Context) (bool, error) {
			d, err := cluster.Clientset.AppsV1().
				Deployments(preset.Namespace).
				Get(ctx, preset.DeploymentName, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			if replicas == 0 {
				pods, err := cluster.Clientset.CoreV1().
					Pods(preset.Namespace).
					List(ctx, metav1.ListOptions{})
				return err == nil && len(pods.Items) == 0, nil
			}
			return d.Status.ReadyReplicas == replicas, nil
		},
	)
	if err != nil {
		t.Fatalf("operator did not reach %d replicas: %v", replicas, err)
	}
}

// replaceWithStuck swaps a pod for a copy of itself, built by the production
// pod builder, whose sidecar can never report started. This is the test-only
// fixture override: the operator has no switch for a broken sidecar, and an
// init container's arguments cannot be edited on a live pod. The copy keeps
// the builder's labels, owner and spec-hash annotation, so against the new
// spec it looks stale exactly like the pod it replaced.
//
// Gap noted for testkit: ctrltest does not run kubelet, so the stuck state
// (startup probe kills, Initialized false) cannot be modelled there; it needs
// a real kubelet, which is why this case lives in e2e.
func (p *pgbr) replaceWithStuck(t *testing.T, src *corev1.Pod, brokenProbe bool) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	spec := src.Spec.DeepCopy()
	spec.NodeName = ""
	sc := &spec.InitContainers[0]
	if sc.Name != sidecarName {
		t.Fatalf("init container 0 of %s is %q, want %s", src.Name, sc.Name, sidecarName)
	}
	if brokenProbe {
		// Alive, but never healthy: kubelet kills it on the startup probe, and
		// SIGTERM makes it exit 0.
		sc.StartupProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{"false"}},
			},
			PeriodSeconds:    1,
			FailureThreshold: 5,
		}
	} else {
		// A plain directory is not a mount point, so the sidecar refuses to start.
		for i, a := range sc.Args {
			if strings.HasPrefix(a, "--data-volume=") {
				sc.Args[i] = "--data-volume=/tmp"
			}
		}
		sc.StartupProbe.PeriodSeconds = 1
		sc.StartupProbe.FailureThreshold = 5
	}

	grace := int64(0)
	if err := p.c.Delete(ctx, src, &client.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
		t.Fatalf("delete %s: %v", src.Name, err)
	}
	waitFor(t, 3*time.Minute, "pod "+src.Name+" gone", func() bool {
		err := p.c.Get(ctx, client.ObjectKeyFromObject(src), &corev1.Pod{})
		return client.IgnoreNotFound(err) == nil && err != nil
	})
	clone := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            src.Name,
			Namespace:       src.Namespace,
			Labels:          src.Labels,
			Annotations:     src.Annotations,
			OwnerReferences: src.OwnerReferences,
		},
		Spec: *spec,
	}
	if err := p.c.Create(ctx, clone); err != nil {
		t.Fatalf("create stuck copy of %s: %v", src.Name, err)
	}
	return clone
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := wait.PollUntilContextCancel(
		ctx,
		2*time.Second,
		true,
		func(context.Context) (bool, error) {
			return cond(), nil
		},
	); err != nil {
		t.Fatalf("timed out waiting for %s: %v", what, err)
	}
}

// testStuckPodEscape checks the corrected stuck-pod semantics. A stuck pod is
// one whose sidecar never reports started: either it keeps crashing, or it is
// alive but unhealthy and kubelet kills it on the startup probe (exit code 0).
// A change to the pod template replaces those pods one at a time. A pod whose sidecar
// has started and that is merely unready is not replaced outside the gated roll.
func (p *pgbr) testStuckPodEscape(t *testing.T) {
	// Part 1: a merely unready pod is not replaced. With the operator stopped,
	// the sidecar's resources change, so every pod is stale; one replica's
	// postmaster is then frozen (its readiness probe fails) before the operator
	// returns.
	_, replicas := p.primaryAndReplicas(t)
	victim := replicas[0]
	p.scaleOperator(t, 0)
	p.bumpSidecarResources(t)
	pid := strings.TrimSpace(mustExec(t, victim, postgresName,
		fmt.Sprintf("head -1 %s/postmaster.pid", shardcontroller.PgDataPath)))
	mustExec(t, victim, postgresName, "kill -STOP "+pid)
	thawed := false
	thaw := func() {
		if !thawed {
			thawed = true
			_, _ = kexec(victim, postgresName, "kill -CONT "+pid)
		}
	}
	t.Cleanup(thaw)
	waitFor(t, 2*time.Minute, "victim to turn unready", func() bool {
		cur := &corev1.Pod{}
		return p.c.Get(context.Background(), client.ObjectKeyFromObject(victim), cur) == nil &&
			!podReady(cur)
	})
	p.scaleOperator(t, 1)

	observe := time.Now().Add(90 * time.Second)
	for time.Now().Before(observe) {
		cur := &corev1.Pod{}
		if err := p.c.Get(
			context.Background(),
			client.ObjectKeyFromObject(victim),
			cur,
		); err != nil ||
			cur.UID != victim.UID {
			t.Fatalf(
				"unready pod %s was replaced (get err %v) while its sidecar had started",
				victim.Name,
				err,
			)
		}
		if podReady(cur) {
			t.Logf("DEGRADED: pod %s became ready again during the observation window", victim.Name)
			break
		}
		if v := cur.Annotations[metadata.AnnotationDrainState]; v != "" {
			t.Fatalf("unready pod %s was drained (%q) outside the gated roll", victim.Name, v)
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf(
		"unready pod %s kept its UID and was not drained for the %s observation window",
		victim.Name,
		"90s",
	)
	thaw()
	p.waitSettled(t)

	// Part 2: stuck pods are replaced one at a time.
	_, replicas = p.primaryAndReplicas(t)
	if len(replicas) < 2 {
		t.Fatalf("want two replicas to make stuck, have %d", len(replicas))
	}
	p.scaleOperator(t, 0)
	stuck := []*corev1.Pod{
		p.replaceWithStuck(t, replicas[0], false),
		p.replaceWithStuck(t, replicas[1], true),
	}
	waitFor(t, 3*time.Minute, "both copies to have restarted the sidecar", func() bool {
		for _, s := range stuck {
			cur := &corev1.Pod{}
			if p.c.Get(context.Background(), client.ObjectKeyFromObject(s), cur) != nil {
				return false
			}
			cs := initStatus(cur, sidecarName)
			if cs == nil || cs.RestartCount < 1 || ptr.Deref(cs.Started, false) {
				return false
			}
		}
		return true
	})
	for _, s := range stuck {
		cur := &corev1.Pod{}
		_ = p.c.Get(context.Background(), client.ObjectKeyFromObject(s), cur)
		cs := initStatus(cur, sidecarName)
		exit := int32(-1)
		if cs.LastTerminationState.Terminated != nil {
			exit = cs.LastTerminationState.Terminated.ExitCode
		}
		t.Logf(
			"stuck pod %s: restarts=%d started=%v lastExit=%d",
			cur.Name,
			cs.RestartCount,
			ptr.Deref(cs.Started, false),
			exit,
		)
		s.UID = cur.UID
	}

	p.bumpSidecarResources(t)
	p.scaleOperator(t, 1)

	stuckUIDs := map[string]bool{string(stuck[0].UID): true, string(stuck[1].UID): true}
	maxInFlight := 0
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(
		ctx,
		time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			pods, err := p.poolPods(ctx)
			if err != nil {
				return false, nil
			}
			inFlight, remaining := 0, 0
			for i := range pods {
				if !stuckUIDs[string(pods[i].UID)] {
					continue
				}
				remaining++
				if !pods[i].DeletionTimestamp.IsZero() ||
					pods[i].Annotations[metadata.AnnotationDrainState] != "" {
					inFlight++
				}
			}
			maxInFlight = max(maxInFlight, inFlight)
			return remaining == 0, nil
		},
	)
	if err != nil {
		p.dump(t)
		t.Fatalf("stuck pods were not replaced: %v", err)
	}
	t.Logf("stuck pods replaced; at most %d were in flight at once", maxInFlight)
	if maxInFlight > 1 {
		t.Errorf("%d stuck pods were being replaced at once, want one at a time", maxInFlight)
	}
	if maxInFlight == 0 {
		t.Errorf("never observed a stuck pod draining or terminating")
	}

	// The stuck pods are gone. The one stale pod left has a started sidecar and
	// belongs to the gated roll, which is a separate behaviour from the escape.
	// It is skipped only when multiorch shows the multigres consensus wedge the
	// fixture can induce, and fails otherwise.
	t.Run("RemainingPodRolls", func(t *testing.T) {
		ok, last := p.trySettled(t, 8*time.Minute)
		if ok {
			return
		}
		p.dump(t)
		// Skip only on the cause, never on the symptom: a stale pod that is
		// not rolled looks the same whether multigres is wedged or the
		// operator's gated roll regressed, and only the first is known.
		wedged, evidence := p.consensusWedge(t)
		if !wedged {
			t.Fatalf("the remaining stale pod did not roll and multiorch shows no consensus "+
				"wedge, so this is not the known multigres defect: %s (%s)", last, evidence)
		}
		t.Skipf("KNOWN MULTIGRES DEFECT, not the gated roll: the fixture took two of three cohort "+
			"members down at once, a consensus rule proposal write was cancelled while waiting for "+
			"synchronous commit, and the undecided proposal leaves current_rule locked, so the "+
			"operator correctly refuses to switch over the primary (%s). Last state: %s",
			evidence, last)
	})
}

// The two multiorch log strings that together identify the multigres consensus
// wedge: the cancelled rule-proposal write (UpdateConsensusRule failed) and the
// loop it leaves behind, one line a second for as long as the proposal stays
// undecided.
const (
	wedgeProposalWriteFailed = "UpdateConsensusRule failed"
	wedgeUndecidedProposal   = "undecided rule proposal"
)

// consensusWedge reports whether the shard's multiorch logs show the wedge
// signature, with the counts as evidence. It requires both strings, so a log
// that has only the symptom (or none of it) is not mistaken for the known
// defect. Logs that cannot be read count as no wedge: a skip must be earned by
// evidence, not granted by a failed fetch.
func (p *pgbr) consensusWedge(t *testing.T) (bool, string) {
	t.Helper()
	pods := &corev1.PodList{}
	if err := p.c.List(context.Background(), pods,
		client.InNamespace(p.ns),
		client.MatchingLabels{
			metadata.LabelMultigresCluster: p.cr.Name,
			metadata.LabelAppComponent:     shardcontroller.MultiorchComponentName,
		},
	); err != nil {
		return false, fmt.Sprintf("list multiorch pods: %v", err)
	}
	if len(pods.Items) == 0 {
		return false, "no multiorch pods found"
	}
	var evidence []string
	wedged := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		// #nosec G204 -- Fixed executables; arguments are test-controlled names.
		out, err := exec.Command("kubectl", "--kubeconfig", cluster.Kubeconfig,
			"logs", "-n", pod.Namespace, pod.Name, "-c", shardcontroller.MultiorchComponentName,
			"--tail=-1").CombinedOutput()
		if err != nil {
			evidence = append(evidence, fmt.Sprintf("%s: logs unreadable: %v", pod.Name, err))
			continue
		}
		logs := string(out)
		failed := strings.Count(logs, wedgeProposalWriteFailed)
		undecided := strings.Count(logs, wedgeUndecidedProposal)
		evidence = append(evidence, fmt.Sprintf("%s: %q x%d, %q x%d",
			pod.Name, wedgeProposalWriteFailed, failed, wedgeUndecidedProposal, undecided))
		if failed > 0 && undecided > 0 {
			wedged = true
		}
	}
	return wedged, strings.Join(evidence, "; ")
}

func initStatus(pod *corev1.Pod, name string) *corev1.ContainerStatus {
	for i := range pod.Status.InitContainerStatuses {
		if pod.Status.InitContainerStatuses[i].Name == name {
			return &pod.Status.InitContainerStatuses[i]
		}
	}
	return nil
}
