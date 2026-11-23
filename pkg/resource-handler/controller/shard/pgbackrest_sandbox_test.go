package shard

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestParseSandboxMode(t *testing.T) {
	for in, want := range map[string]string{
		"":         SandboxRequired,
		"required": SandboxRequired,
		"auto":     SandboxAuto,
		"off":      SandboxOff,
	} {
		got, err := ParseSandboxMode(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}

	for _, bad := range []string{"Required", "on", "true", "bogus", " auto"} {
		_, err := ParseSandboxMode(bad)
		require.Error(t, err, bad)
		for _, accepted := range []string{`"required"`, `"auto"`, `"off"`} {
			assert.Contains(t, err.Error(), accepted)
		}
	}
}

func sandboxArg(t *testing.T, args []string) string {
	t.Helper()
	var found []string
	for _, a := range args {
		if len(a) > len("--sandbox=") && a[:len("--sandbox=")] == "--sandbox=" {
			found = append(found, a)
		}
	}
	require.Len(t, found, 1, "exactly one --sandbox argument")
	return found[0]
}

// The sandbox flag appears on the pgbackrestd container of pool pods and Lite
// pods with the configured value, an unset mode renders as required, and the
// mode is part of the spec hash so a flip rolls the pod.
func TestPgBackRestSandboxModeInPods(t *testing.T) {
	cases := map[string]struct {
		opts []PodOption
		want string
	}{
		"default":  {nil, "--sandbox=required"},
		"empty":    {[]PodOption{WithSandboxMode("")}, "--sandbox=required"},
		"required": {[]PodOption{WithSandboxMode(SandboxRequired)}, "--sandbox=required"},
		"auto":     {[]PodOption{WithSandboxMode(SandboxAuto)}, "--sandbox=auto"},
		"off":      {[]PodOption{WithSandboxMode(SandboxOff)}, "--sandbox=off"},
	}
	hashes := map[string]string{}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := sidecarTestShard()
			pool, err := BuildPoolPod(
				s,
				"main",
				"z1",
				newTestPoolSpec(),
				0,
				testScheme(),
				tc.opts...)
			require.NoError(t, err)
			lite, err := BuildLitePod(s, "main", "z1", newTestPoolSpec(), testScheme(), tc.opts...)
			require.NoError(t, err)

			for _, c := range []string{
				sandboxArg(t, containerNamed(t, pool, PgBackRestSidecarContainerName).Args),
				sandboxArg(t, containerNamed(t, lite, PgBackRestSidecarContainerName).Args),
			} {
				assert.Equal(t, tc.want, c)
			}
			hashes[name] = pool.Annotations[metadata.AnnotationSpecHash]
		})
	}
	assert.Equal(t, hashes["default"], hashes["required"])
	assert.Equal(t, hashes["empty"], hashes["required"])
	assert.NotEqual(t, hashes["required"], hashes["auto"])
	assert.NotEqual(t, hashes["required"], hashes["off"])
	assert.NotEqual(t, hashes["auto"], hashes["off"])
}

// A mode flip is drift for pods built under another mode, through the
// reconciler's own option plumbing.
func TestPodNeedsUpdateOnSandboxModeFlip(t *testing.T) {
	s := sidecarTestShard()
	pool := newTestPoolSpec()
	pod, err := BuildPoolPod(
		s,
		"main",
		"z1",
		pool,
		0,
		testScheme(),
		WithSandboxMode(SandboxRequired),
	)
	require.NoError(t, err)

	r := &ShardReconciler{Scheme: testScheme(), PgBackRestSandbox: SandboxRequired}
	assert.False(t, podNeedsUpdate(pod, s, "main", "z1", pool, 0, r.Scheme, r.podOptions()...))
	r.PgBackRestSandbox = SandboxAuto
	assert.True(t, podNeedsUpdate(pod, s, "main", "z1", pool, 0, r.Scheme, r.podOptions()...))
	r.PgBackRestSandbox = SandboxOff
	assert.True(t, podNeedsUpdate(pod, s, "main", "z1", pool, 0, r.Scheme, r.podOptions()...))
}
