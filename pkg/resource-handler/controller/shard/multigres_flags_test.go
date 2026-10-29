package shard

import (
	"embed"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// testdata/multigres-flags/<binary>.txt lists every flag name the multigres
// binary accepts, one per line: pgctld (server), multipooler, minigres and
// pgbackrestd (serve). They are derived from the flag registrations of a
// multigres checkout and are what keeps a flag removed or renamed upstream
// from reaching a pod as "unknown flag". Regenerate them after a multigres
// bump with:
//
//	scripts/refresh-multigres-flags.sh /path/to/multigres
//
// and read the diff: a removed flag that a container still passes fails
// TestContainerArgsAreAcceptedByMultigres.
//
//go:embed testdata/multigres-flags/*.txt
var multigresFlagFiles embed.FS

func acceptedFlags(t *testing.T, binary string) map[string]bool {
	t.Helper()
	data, err := multigresFlagFiles.ReadFile("testdata/multigres-flags/" + binary + ".txt")
	require.NoError(t, err)
	flags := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			flags[line] = true
		}
	}
	require.NotEmpty(t, flags, binary)
	return flags
}

// flagNames returns the flag names in args, whether written --name=value or
// --name value. Subcommands and flag values do not start with "--".
func flagNames(args []string) []string {
	var names []string
	for _, a := range args {
		if !strings.HasPrefix(a, "--") {
			continue
		}
		names = append(names, strings.SplitN(a, "=", 2)[0])
	}
	return names
}

func containerNamed(t *testing.T, pod *corev1.Pod, name string) corev1.Container {
	t.Helper()
	for _, c := range append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...) {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("container %q not found", name)
	return corev1.Container{}
}

func TestContainerArgsAreAcceptedByMultigres(t *testing.T) {
	variants := map[string]func(*multigresv1alpha1.Shard){
		"s3 static credentials and encryption": func(*multigresv1alpha1.Shard) {},
		"s3 web identity": func(s *multigresv1alpha1.Shard) {
			s.Spec.SidecarBackupLocation.S3.CredentialsSecret = ""
			s.Spec.SidecarBackupLocation.Encryption = nil
		},
		"filesystem": func(s *multigresv1alpha1.Shard) {
			s.Spec.SidecarBackupLocation = &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeFilesystem,
			}
		},
		"no backup location": func(s *multigresv1alpha1.Shard) {
			s.Spec.SidecarBackupLocation = nil
		},
		"internal and topology TLS, public certificate": func(s *multigresv1alpha1.Shard) {
			s.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)}
			s.Spec.GlobalTopoServer.ClientCertSecret = "topo-client"
			s.Spec.GlobalTopoServer.CASecret = "topo-ca"
			s.Spec.CertCommonName = "db.example.com"
		},
		"initdb args": func(s *multigresv1alpha1.Shard) {
			s.Spec.InitdbArgs = "--wal-segsize=64"
		},
	}

	for name, mutate := range variants {
		t.Run(name, func(t *testing.T) {
			s := sidecarTestShard()
			mutate(s)

			pool, err := BuildPoolPod(s, "main", "z1", newTestPoolSpec(), 0, testScheme())
			require.NoError(t, err)
			lite, err := BuildLitePod(s, "main", "z1", newTestPoolSpec(), testScheme())
			require.NoError(t, err)

			cases := []struct {
				pod       *corev1.Pod
				container string
				binary    string
			}{
				{pool, "postgres", "pgctld"},
				{pool, "multipooler", "multipooler"},
				{pool, PgBackRestSidecarContainerName, "pgbackrestd"},
				{lite, "postgres", "pgctld"},
				{lite, "minigres", "minigres"},
				{lite, PgBackRestSidecarContainerName, "pgbackrestd"},
			}
			for _, tc := range cases {
				accepted := acceptedFlags(t, tc.binary)
				c := containerNamed(t, tc.pod, tc.container)
				names := flagNames(c.Args)
				require.NotEmpty(t, names, "%s %s", tc.pod.Name, tc.container)
				for _, f := range names {
					assert.True(t, accepted[f],
						"%s passes %s, which %s does not accept", tc.container, f, tc.binary)
				}
			}
		})
	}
}

// The flags multigres refuses to start without, checked against the argument
// lists the operator builds.
func TestContainerArgsCarryTheRequiredMultigresFlags(t *testing.T) {
	pod := buildSidecarPod(t, sidecarTestShard())
	lite, err := BuildLitePod(
		sidecarTestShard(), "main", "z1", newTestPoolSpec(), testScheme(),
	)
	require.NoError(t, err)

	for name, c := range map[string]corev1.Container{
		"multipooler": containerNamed(t, pod, "multipooler"),
		"minigres":    containerNamed(t, lite, "minigres"),
	} {
		assert.Contains(t, c.Args, "--pgbackrest-control-socket="+PgBackRestControlSocketPath, name)
		assert.Contains(t, c.Args, "--pgbackrest-wal-socket="+PgBackRestWALSocketPath, name)
	}

	sidecar := containerNamed(t, pod, PgBackRestSidecarContainerName)
	assert.Contains(t, sidecar.Args, "--data-volume="+DataMountPath)
}

// --sandbox is a flag pgbackrestd accepts and carries the configured mode on
// every pgbackrestd container the operator builds.
func TestPgBackRestSandboxFlagIsAcceptedWithConfiguredValue(t *testing.T) {
	require.True(t, acceptedFlags(t, "pgbackrestd")["--sandbox"],
		"refresh testdata/multigres-flags/pgbackrestd.txt from a multigres that has --sandbox")

	for _, mode := range []string{SandboxRequired, SandboxAuto, SandboxOff} {
		pool, err := BuildPoolPod(sidecarTestShard(), "main", "z1", newTestPoolSpec(), 0,
			testScheme(), WithSandboxMode(mode))
		require.NoError(t, err)
		lite, err := BuildLitePod(sidecarTestShard(), "main", "z1", newTestPoolSpec(),
			testScheme(), WithSandboxMode(mode))
		require.NoError(t, err)
		for name, pod := range map[string]*corev1.Pod{"pool": pool, "lite": lite} {
			c := containerNamed(t, pod, PgBackRestSidecarContainerName)
			assert.Contains(t, c.Args, "--sandbox="+mode, "%s %s", name, mode)
		}
	}
}

func TestMultigresFlagGoldenListsDropRemovedFlags(t *testing.T) {
	removed := map[string][]string{
		"pgctld": {"--pgbackrest-sidecar", "--pgbackrest-port", "--pgbackrest-cert-dir"},
		"multipooler": {
			"--pgbackrest-port", "--pgbackrest-cert-file", "--pgbackrest-key-file",
			"--pgbackrest-ca-file", "--pgbackrest-cipher-key-file",
		},
	}
	for binary, flags := range removed {
		accepted := acceptedFlags(t, binary)
		for _, f := range flags {
			assert.False(t, accepted[f], "%s golden list still has %s", binary, f)
		}
	}
}
