package postgresconfig

import "fmt"

// ReloadMarkerGUC is a synthetic placeholder GUC StampAndSplit stamps into every
// rendered postgresql.conf. Its value is the reload-hash, so it moves on ANY
// reload-safe change — crucially including one that only REMOVES a setting.
//
// It exists to make an in-place reload verifiable for removals. The multipooler's
// ReloadConfig gate only checks that the expected settings are PRESENT in the
// mounted file with the desired value; it cannot observe that a setting the render
// dropped is still there. Without the marker, a change that only removes a
// reload-safe setting — whose remaining expected settings a stale, not-yet-synced
// ConfigMap mount still satisfies — would pass the gate against the stale file,
// reload the OLD file (removed setting still present, so not reverted), and be
// stamped current: the revert silently lost until an unrelated reload or restart.
//
// With the marker in expected_settings, a stale file carries the PREVIOUS marker
// value and fails the gate, so the reload is retried until the kubelet syncs the
// new file — at which point the removal is really applied. The marker's value is
// the reload-hash rather than a per-setting value precisely so it also covers the
// removal case, where no individual expected value distinguishes the new file.
//
// It is a two-part (namespaced) name, which PostgreSQL accepts as a reload-safe
// placeholder custom variable even without the defining extension loaded, so it
// appears in pg_file_settings with applied=true and never needs a restart.
const ReloadMarkerGUC = "multigres.config_reload_marker"

// reloadMarkerLine renders the marker's postgresql.conf line for a given
// reload-hash. StampAndSplit appends it to every rendered config.
func reloadMarkerLine(reloadHash string) string {
	return fmt.Sprintf(
		"\n# Operator-managed config-version marker; do not edit.\n%s = '%s'\n",
		ReloadMarkerGUC,
		reloadHash,
	)
}
