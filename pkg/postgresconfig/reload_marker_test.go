package postgresconfig

import (
	"strings"
	"testing"
)

func TestSplitConfigStampsMarker(t *testing.T) {
	const conf = `work_mem = '4MB'
max_wal_size = '1GB'
`
	rendered, split := SplitConfig(conf)

	// The marker line is appended to the returned file with value == reload-hash.
	wantLine := ReloadMarkerGUC + " = '" + split.ReloadHash + "'"
	if !strings.Contains(rendered, wantLine) {
		t.Errorf("rendered config missing marker line %q:\n%s", wantLine, rendered)
	}

	// The marker is present in the expected-settings map, unquoted, == reload-hash.
	if got := split.ReloadSettings[ReloadMarkerGUC]; got != split.ReloadHash {
		t.Errorf(
			"ReloadSettings[%q] = %q, want reload-hash %q",
			ReloadMarkerGUC,
			got,
			split.ReloadHash,
		)
	}

	// The reload-hash covers the real settings only, not the marker: hashing the
	// reload settings with the marker removed must reproduce the reload-hash.
	delete(split.ReloadSettings, ReloadMarkerGUC)
	if h := hashSettings(split.ReloadSettings); h != split.ReloadHash {
		t.Errorf(
			"reload-hash includes the marker: %s over settings-without-marker != %s",
			h,
			split.ReloadHash,
		)
	}
}

func TestSplitConfigMarkerOnAllRestartConfig(t *testing.T) {
	// A config with only restart-only settings still gets a marker, so even an
	// all-restart render carries a version marker in its reload settings.
	_, split := SplitConfig("shared_buffers = '128MB'\n")
	if split.ReloadSettings[ReloadMarkerGUC] != split.ReloadHash {
		t.Errorf("marker not stamped for an all-restart config: %v", split.ReloadSettings)
	}
}

// TestReloadMarkerDetectsRemoval is the core guard for the removal-only fix. When
// a change only REMOVES a reload-safe setting, every remaining expected setting is
// unchanged, so a stale (not-yet-synced) mounted file still satisfies them — the
// pooler's presence-only gate cannot tell the file is stale. The marker closes
// that gap: because its value is the reload-hash, which moves when the reload-safe
// partition changes, the removal shifts the expected marker so a stale file
// (carrying the previous marker) fails the gate and the reload is retried until
// the kubelet syncs.
func TestReloadMarkerDetectsRemoval(t *testing.T) {
	const withParam = `work_mem = '4MB'
random_page_cost = '1.1'
`
	// Same config with the reload-safe random_page_cost removed entirely.
	const withoutParam = `work_mem = '4MB'
`

	_, before := SplitConfig(withParam)
	_, after := SplitConfig(withoutParam)

	// Neither config has a restart-only setting, so the restart-hash is unchanged
	// (this stays a reload, not a pod recreation).
	if before.RestartHash != after.RestartHash {
		t.Errorf("restart-hash moved on a reload-only removal: %s -> %s",
			before.RestartHash, after.RestartHash)
	}

	// The removal changed the reload-safe partition, so the reload-hash — and thus
	// the marker value — must move.
	if before.ReloadHash == after.ReloadHash {
		t.Fatalf("reload-hash did not move when a reload-safe setting was removed (still %s)",
			before.ReloadHash)
	}
	beforeMarker := before.ReloadSettings[ReloadMarkerGUC]
	afterMarker := after.ReloadSettings[ReloadMarkerGUC]
	if beforeMarker == afterMarker {
		t.Fatalf("marker did not move on removal: %q", afterMarker)
	}

	// The crux: every non-marker expected setting of the post-removal config is
	// still satisfied by the pre-removal (stale) file — so without the marker the
	// gate would pass against the stale file. With the marker, the stale file's
	// old marker value fails the new expectation.
	staleFile := parseEffectiveConfig(withParam)
	for name, want := range after.ReloadSettings {
		staleVal, present := staleFile[name]
		if name == ReloadMarkerGUC {
			if present {
				t.Errorf("stale file unexpectedly already carries the marker")
			}
			continue // the marker is what the stale file cannot satisfy
		}
		if !present || unquoteValue(staleVal) != want {
			t.Errorf(
				"non-marker setting %q=%q is NOT satisfied by the stale file (val=%q present=%v); "+
					"the removal test only holds if the stale file satisfies every non-marker expectation",
				name,
				want,
				staleVal,
				present,
			)
		}
	}
}
