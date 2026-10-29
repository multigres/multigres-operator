//go:build e2e

package minigres_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMinigresRetriesPostgresAfterProlongedFailure exercises the same failed-start
// budget as a full disk, without exhausting storage on the shared Kind host.
func TestMinigresRetriesPostgresAfterProlongedFailure(t *testing.T) {
	cr, pod := startLiteRuntime(t, "")
	ns := cr.Namespace
	sqlEventually(
		t,
		ns,
		pod.Name,
		"CREATE TABLE recovery_e2e (id integer PRIMARY KEY)",
		"CREATE TABLE",
	)
	sqlMust(t, ns, pod.Name, "INSERT INTO recovery_e2e VALUES (1)", "INSERT 0 1")
	dataDir, err := sql(ns, pod.Name, "SHOW data_directory")
	require.NoError(t, err)
	require.NotEmpty(t, dataDir)
	pvc := dataPVC(t, ns, pod)
	original := minigresStatus(t, getPod(t, ns, pod.Name))

	// A missing preload library makes every PostgreSQL start fail. Preserve the
	// original configuration so removing this fault is the only recovery action.
	const inject = `cp "$1/postgresql.auto.conf" "$1/postgresql.auto.conf.e2e-original"
printf "\nshared_preload_libraries = 'lite_e2e_missing_library'\n" >> "$1/postgresql.auto.conf"
pg_ctl -D "$1" -m fast -w stop`
	const restore = `cp "$1/postgresql.auto.conf.e2e-original" "$1/postgresql.auto.conf"`
	t.Cleanup(func() {
		_, err := kubectl(
			ns,
			"exec",
			pod.Name,
			"-c",
			"postgres",
			"--",
			"sh",
			"-ec",
			restore,
			"sh",
			dataDir,
		)
		if err != nil {
			t.Logf("restore test PostgreSQL configuration: %v", err)
		}
	})
	_, err = kubectl(
		ns,
		"exec",
		pod.Name,
		"-c",
		"postgres",
		"--",
		"sh",
		"-ec",
		inject,
		"sh",
		dataDir,
	)
	require.NoError(t, err)

	// The runtime normally quarantines after five minutes of failed starts.
	// Keep the fault for six minutes and reject container/pod replacement, which
	// would reset that timer and could otherwise hide a missing timeout override.
	deadline := time.Now().Add(6 * time.Minute)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	lastLog := time.Now()
	for time.Now().Before(deadline) {
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-ticker.C:
		}
		current := getPod(t, ns, pod.Name)
		require.Equal(t, pod.UID, current.UID)
		status := minigresStatus(t, current)
		require.Equal(t, original.ContainerID, status.ContainerID)
		require.Equal(t, original.RestartCount, status.RestartCount)
		_, err := sql(ns, pod.Name, "SELECT 1")
		require.Error(t, err, "PostgreSQL must remain down while the startup fault is present")
		if time.Since(lastLog) >= time.Minute {
			t.Log("PostgreSQL startup fault remains active; Minigres has not restarted")
			lastLog = time.Now()
		}
	}
	_, err = kubectl(
		ns,
		"exec",
		pod.Name,
		"-c",
		"postgres",
		"--",
		"sh",
		"-ec",
		restore,
		"sh",
		dataDir,
	)
	require.NoError(t, err)
	sqlEventually(t, ns, pod.Name, "SELECT count(*) FROM recovery_e2e", "1")
	sqlMust(t, ns, pod.Name, "INSERT INTO recovery_e2e VALUES (2)", "INSERT 0 1")
	sqlMust(t, ns, pod.Name, "SELECT count(*) FROM recovery_e2e", "2")
	current := getPod(t, ns, pod.Name)
	require.Equal(t, pod.UID, current.UID)
	require.Equal(t, original.ContainerID, minigresStatus(t, current).ContainerID)
	require.Equal(t, original.RestartCount, minigresStatus(t, current).RestartCount)
	require.Equal(t, pvc.UID, dataPVC(t, ns, current).UID)
	t.Log(
		"SQL recovered after more than five minutes of failed PostgreSQL starts, without pod/container replacement or data loss",
	)
}
