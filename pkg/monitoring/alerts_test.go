package monitoring

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

// PROMTOOL can point to a standalone binary when Prometheus is not installed.
func TestTopologyAlertRules(t *testing.T) {
	binary := os.Getenv("PROMTOOL")
	if binary == "" {
		var err error
		binary, err = exec.LookPath("promtool")
		if err != nil {
			t.Skip("set PROMTOOL or install promtool to test alert evaluation")
		}
	}
	data, err := os.ReadFile("../../config/monitoring/prometheus-rules.yaml")
	require.NoError(t, err)
	var resource struct {
		Spec struct {
			Groups []any `json:"groups"`
		} `json:"spec"`
	}
	require.NoError(t, yaml.Unmarshal(data, &resource))
	rules, err := yaml.Marshal(resource.Spec)
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "rules.yaml"), rules, 0o600))
	fixture, err := os.ReadFile("testdata/topology_alerts.yaml")
	require.NoError(t, err)
	// #nosec G703 -- dir comes from t.TempDir and the filename is constant.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tests.yaml"), fixture, 0o600))
	// #nosec G204 G702 -- PROMTOOL is an explicit test-runner binary, never application input.
	cmd := exec.CommandContext(t.Context(), binary, "test", "rules", "tests.yaml")
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}
