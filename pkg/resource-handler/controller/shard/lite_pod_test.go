package shard

import (
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func newTestLiteShard() *multigresv1alpha1.Shard {
	shard := newTestShard()
	shard.Spec.Mode = multigresv1alpha1.ModeLite
	shard.Spec.Images.Multipooler = "multigres:test"
	shard.Spec.GlobalTopoServer = multigresv1alpha1.GlobalTopoServerRef{
		Address:  "global-topo:2379",
		RootPath: "/multigres/default/test-cluster/global",
	}
	shard.Spec.LogLevels.Multipooler = "info"
	return shard
}

func TestBuildLiteContainer_Args(t *testing.T) {
	base := []string{
		"minigres",
		"--http-port=15200",
		"--grpc-port=15270",
		"--pg-port=15432",
		"--pooler-dir=" + PoolerDirMountPath,
		"--socket-file=" + PostgresSocketPath,
		"--topo-global-server-addresses=global-topo:2379",
		"--topo-global-root=/multigres/default/test-cluster/global",
		"--cell=z1",
		"--database=postgres",
		"--table-group=default",
		"--shard=0-inf",
		"--service-id=p-test",
		"--pgctld-addr=localhost:15470",
		"--log-level=info",
		"--grpc-server-keepalive-enforcement-policy-permit-without-stream=true",
	}
	hostname := "--hostname=$(POD_IP)"

	tests := map[string]struct {
		mutate func(*multigresv1alpha1.Shard)
		want   []string
	}{
		"no optional settings": {
			want: append(slices.Clone(base), hostname),
		},
		"backup configured": {
			mutate: func(s *multigresv1alpha1.Shard) {
				s.Spec.Backup = &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
				}
			},
			want: append(slices.Clone(base),
				"--pgbackrest-cert-file="+PgBackRestCertMountPath+"/pgbackrest.crt",
				"--pgbackrest-key-file="+PgBackRestCertMountPath+"/pgbackrest.key",
				"--pgbackrest-ca-file="+PgBackRestCertMountPath+"/ca.crt",
				hostname,
			),
		},
		"backup encryption configured": {
			mutate: func(s *multigresv1alpha1.Shard) {
				s.Spec.Backup = &multigresv1alpha1.BackupConfig{
					Type:       multigresv1alpha1.BackupTypeFilesystem,
					Encryption: &multigresv1alpha1.BackupEncryptionConfig{SecretName: "cipher"},
				}
			},
			want: append(slices.Clone(base),
				"--pgbackrest-cert-file="+PgBackRestCertMountPath+"/pgbackrest.crt",
				"--pgbackrest-key-file="+PgBackRestCertMountPath+"/pgbackrest.key",
				"--pgbackrest-ca-file="+PgBackRestCertMountPath+"/ca.crt",
				"--pgbackrest-cipher-key-file="+PgBackRestCipherKeyFilePath,
				hostname,
			),
		},
		"internal TLS enabled": {
			mutate: func(s *multigresv1alpha1.Shard) {
				s.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)}
			},
			want: append(slices.Clone(base),
				"--grpc-cert", ShardTLSCertFile,
				"--grpc-key", ShardTLSKeyFile,
				"--grpc-ca", ShardTLSCAFile,
				"--grpc-server-ca", ShardTLSCAFile,
				hostname,
			),
		},
		"topology client TLS configured": {
			mutate: func(s *multigresv1alpha1.Shard) {
				s.Spec.GlobalTopoServer.ClientCertSecret = "topo-client"
				s.Spec.GlobalTopoServer.CASecret = "topo-ca"
			},
			want: append(append(slices.Clone(base),
				multigresv1alpha1.TopoClientTLSArgs()...),
				hostname,
			),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			shard := newTestLiteShard()
			if tc.mutate != nil {
				tc.mutate(shard)
			}
			c := buildLiteContainer(shard, newTestPoolSpec(), "main", "z1", "p-test")
			if diff := cmp.Diff(tc.want, c.Args); diff != "" {
				t.Errorf("args mismatch (-want +got):\n%s", diff)
			}
			for _, arg := range c.Args {
				if arg == "--pg-port=5432" || arg == "--service-map=grpc-pooler" {
					t.Errorf("unexpected multipooler flag %q", arg)
				}
			}
		})
	}
}

func TestBuildLiteContainer_ImagePortsAndProbes(t *testing.T) {
	c := buildLiteContainer(newTestLiteShard(), newTestPoolSpec(), "main", "z1", "p-test")

	if c.Name != "minigres" {
		t.Errorf("name = %q, want minigres", c.Name)
	}
	if c.Image != "multigres:test" {
		t.Errorf("image = %q, want the shard's multipooler image", c.Image)
	}

	wantPorts := []corev1.ContainerPort{
		{Name: "http", ContainerPort: 15200, Protocol: corev1.ProtocolTCP},
		{Name: "grpc", ContainerPort: 15270, Protocol: corev1.ProtocolTCP},
		{Name: "postgres", ContainerPort: 15432, Protocol: corev1.ProtocolTCP},
	}
	if diff := cmp.Diff(wantPorts, c.Ports); diff != "" {
		t.Errorf("ports mismatch (-want +got):\n%s", diff)
	}

	httpGet := func(path string) corev1.ProbeHandler {
		return corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: path,
			Port: intstr.FromInt32(15200),
		}}
	}
	for name, probe := range map[string]struct {
		got  *corev1.Probe
		want corev1.ProbeHandler
	}{
		"startup":   {got: c.StartupProbe, want: httpGet("/live")},
		"liveness":  {got: c.LivenessProbe, want: httpGet("/live")},
		"readiness": {got: c.ReadinessProbe, want: httpGet("/ready")},
	} {
		if probe.got == nil {
			t.Errorf("%s probe missing", name)
			continue
		}
		if diff := cmp.Diff(probe.want, probe.got.ProbeHandler); diff != "" {
			t.Errorf("%s probe mismatch (-want +got):\n%s", name, diff)
		}
	}
}

func TestBuildLiteContainer_HostnameFromPodIP(t *testing.T) {
	c := buildLiteContainer(newTestLiteShard(), newTestPoolSpec(), "main", "z1", "p-test")

	if !slices.Contains(c.Args, "--hostname=$(POD_IP)") {
		t.Errorf("args %v do not advertise the pod IP", c.Args)
	}
	idx := slices.IndexFunc(c.Env, func(e corev1.EnvVar) bool { return e.Name == "POD_IP" })
	if idx < 0 {
		t.Fatalf("POD_IP env var missing: %v", c.Env)
	}
	ref := c.Env[idx].ValueFrom
	if ref == nil || ref.FieldRef == nil || ref.FieldRef.FieldPath != "status.podIP" {
		t.Errorf("POD_IP source = %#v, want fieldRef status.podIP", ref)
	}
}

func TestBuildLitePod(t *testing.T) {
	shard := newTestLiteShard()
	shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket:             "backups",
			ServiceAccountName: "backup-sa",
		},
	}
	pool := newTestPoolSpec()

	pod, err := BuildLitePod(shard, "main", "z1", pool, testScheme())
	if err != nil {
		t.Fatalf("BuildLitePod() error = %v", err)
	}

	if want := BuildPoolPodName(shard, "main", "z1", 0); pod.Name != want {
		t.Errorf("name = %q, want %q", pod.Name, want)
	}
	for k, want := range buildPoolLabelsWithCell(shard, "main", "z1") {
		if got := pod.Labels[k]; got != want {
			t.Errorf("label %q = %q, want %q", k, got, want)
		}
	}
	if got := pod.Labels[metadata.LabelMultigresGatewayRole]; got != metadata.GatewayRoleLite {
		t.Errorf("gateway-role label = %q, want %q", got, metadata.GatewayRoleLite)
	}

	if len(pod.Spec.ReadinessGates) != 0 {
		t.Errorf("readiness gates = %v, want none", pod.Spec.ReadinessGates)
	}
	if pod.Spec.Hostname != "" || pod.Spec.Subdomain != "" {
		t.Errorf(
			"hostname/subdomain = %q/%q, want unset",
			pod.Spec.Hostname,
			pod.Spec.Subdomain,
		)
	}
	if pod.Spec.ServiceAccountName != "backup-sa" {
		t.Errorf("service account = %q, want backup-sa", pod.Spec.ServiceAccountName)
	}

	if len(pod.Spec.Volumes) == 0 || pod.Spec.Volumes[0].PersistentVolumeClaim == nil ||
		pod.Spec.Volumes[0].Name != DataVolumeName ||
		pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName !=
			BuildPoolDataPVCName(shard, "main", "z1", 0) {
		t.Errorf("first volume = %#v, want the index 0 data PVC", pod.Spec.Volumes)
	}
	if diff := cmp.Diff(buildPoolVolumes(shard, "z1"), pod.Spec.Volumes[1:]); diff != "" {
		t.Errorf("shared volumes mismatch (-want +got):\n%s", diff)
	}

	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("init containers = %d, want the pgctld sidecar", len(pod.Spec.InitContainers))
	}
	pgctld := pod.Spec.InitContainers[0]
	if diff := cmp.Diff(buildPgctldSidecar(shard, pool), pgctld); diff != "" {
		t.Errorf("pgctld sidecar mismatch (-want +got):\n%s", diff)
	}
	if len(pgctld.Args) == 0 || pgctld.Args[0] != "server" ||
		slices.Contains(pgctld.Args, "init") || slices.Contains(pgctld.Command, "init") {
		t.Errorf("pgctld command %v %v must run server and never init",
			pgctld.Command, pgctld.Args)
	}

	names := make([]string, 0, len(pod.Spec.Containers))
	for _, c := range pod.Spec.Containers {
		names = append(names, c.Name)
	}
	if diff := cmp.Diff([]string{"minigres", "postgres-exporter"}, names); diff != "" {
		t.Errorf("containers mismatch (-want +got):\n%s", diff)
	}

	wantHash := ComputeSpecHash(pod)
	if got := pod.Annotations[metadata.AnnotationSpecHash]; got != wantHash {
		t.Errorf("spec-hash = %q, want %q", got, wantHash)
	}
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Name != shard.Name {
		t.Errorf("owner references = %v, want the shard", pod.OwnerReferences)
	}
}

func TestBuildLitePod_SpecHashFollowsRestartConfig(t *testing.T) {
	shard := newTestLiteShard()
	pool := newTestPoolSpec()

	shard.Annotations = map[string]string{metadata.AnnotationPostgresConfigHash: "a"}
	before, err := BuildLitePod(shard, "main", "z1", pool, testScheme())
	if err != nil {
		t.Fatalf("BuildLitePod() error = %v", err)
	}
	shard.Annotations[metadata.AnnotationPostgresConfigHash] = "b"
	after, err := BuildLitePod(shard, "main", "z1", pool, testScheme())
	if err != nil {
		t.Fatalf("BuildLitePod() error = %v", err)
	}
	if before.Annotations[metadata.AnnotationSpecHash] ==
		after.Annotations[metadata.AnnotationSpecHash] {
		t.Error("spec-hash did not change with the restart config hash")
	}
}

func TestLitePodPriorityPolicyTriggersDrift(t *testing.T) {
	r := &ShardReconciler{Scheme: testScheme()}
	shard, pool := newTestLiteShard(), newTestPoolSpec()
	before, err := r.buildLitePod(shard, "main", "z1", pool)
	if err != nil {
		t.Fatal(err)
	}
	r.LitePodPriorityClassName = "lite-database"
	after, err := r.buildLitePod(shard, "main", "z1", pool)
	if err != nil {
		t.Fatal(err)
	}
	if after.Spec.PriorityClassName != "lite-database" {
		t.Fatalf("priority class = %q", after.Spec.PriorityClassName)
	}
	if before.Annotations[metadata.AnnotationSpecHash] == after.Annotations[metadata.AnnotationSpecHash] {
		t.Fatal("changing the priority class must trigger Lite pod replacement")
	}
	if after.Annotations[metadata.AnnotationSpecHash] != ComputeSpecHash(after) {
		t.Fatal("desired-pod hash does not include the applied priority policy")
	}
	for key, want := range map[string]string{
		"karpenter.sh/do-not-disrupt":                    "true",
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
	} {
		if after.Annotations[key] != want {
			t.Errorf("annotation %s = %q, want %q", key, after.Annotations[key], want)
		}
	}
}
