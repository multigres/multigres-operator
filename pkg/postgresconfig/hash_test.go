package postgresconfig

import "testing"

// baseConf is a minimal rendered postgresql.conf covering a restart param
// (shared_buffers, postmaster), a couple of reload params (work_mem/user,
// max_wal_size/sighup), comments, and inline "# ..." trailers.
const baseConf = `# rendered by test
shared_buffers = '128MB'		# (change requires restart)
work_mem = '4MB'
max_wal_size = '1GB'			# sighup
`

func TestSplitHashesReloadOnlyChangeLeavesRestartHash(t *testing.T) {
	base := SplitHashes(baseConf)

	// Change only work_mem (a reload-safe/user param).
	changed := SplitHashes(`# rendered by test
shared_buffers = '128MB'		# (change requires restart)
work_mem = '8MB'
max_wal_size = '1GB'			# sighup
`)

	if changed.RestartHash != base.RestartHash {
		t.Errorf("restart-hash moved on a reload-only change: %s -> %s", base.RestartHash, changed.RestartHash)
	}
	if changed.ReloadHash == base.ReloadHash {
		t.Errorf("reload-hash did not move on a work_mem change (still %s)", base.ReloadHash)
	}
}

func TestSplitHashesRestartChangeMovesRestartHash(t *testing.T) {
	base := SplitHashes(baseConf)

	// Change shared_buffers (a postmaster/restart param).
	changed := SplitHashes(`# rendered by test
shared_buffers = '256MB'		# (change requires restart)
work_mem = '4MB'
max_wal_size = '1GB'			# sighup
`)

	if changed.RestartHash == base.RestartHash {
		t.Errorf("restart-hash did not move on a shared_buffers change (still %s)", base.RestartHash)
	}
	if changed.ReloadHash != base.ReloadHash {
		t.Errorf("reload-hash moved on a restart-only change: %s -> %s", base.ReloadHash, changed.ReloadHash)
	}
}

func TestSplitHashesCosmeticEditsMoveNeither(t *testing.T) {
	base := SplitHashes(baseConf)

	// Reordered, differently commented, extra blank lines, different inline
	// comment text — same effective values.
	cosmetic := SplitHashes(`
# a different header

max_wal_size = '1GB'   # reload-safe, different comment
work_mem = '4MB'

shared_buffers = '128MB'
`)

	if cosmetic.RestartHash != base.RestartHash {
		t.Errorf("restart-hash moved on a cosmetic-only edit: %s -> %s", base.RestartHash, cosmetic.RestartHash)
	}
	if cosmetic.ReloadHash != base.ReloadHash {
		t.Errorf("reload-hash moved on a cosmetic-only edit: %s -> %s", base.ReloadHash, cosmetic.ReloadHash)
	}
}

func TestSplitHashesLastWins(t *testing.T) {
	// A later assignment is the effective value; hashing a config whose final
	// work_mem is 8MB must equal one that sets it to 8MB once.
	dup := SplitHashes(`work_mem = '4MB'
work_mem = '8MB'
`)
	single := SplitHashes(`work_mem = '8MB'
`)
	if dup.ReloadHash != single.ReloadHash {
		t.Errorf("last-wins not honored: dup=%s single=%s", dup.ReloadHash, single.ReloadHash)
	}
}

func TestStripInlineComment(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"'128MB'		# (change requires restart)", "'128MB'\t\t"},
		{"'HIGH:MEDIUM:+3DES:!aNULL' # allowed SSL ciphers", "'HIGH:MEDIUM:+3DES:!aNULL' "},
		{"on", "on"},
		{"'%h %m [%p] %q%u@%d '", "'%h %m [%p] %q%u@%d '"},
		// '#' inside a single-quoted value is preserved.
		{"'a#b'", "'a#b'"},
		// escaped '' inside a quoted value does not end the string.
		{"'it''s #1' # trailer", "'it''s #1' "},
	}
	for _, tt := range tests {
		if got := stripInlineComment(tt.in); got != tt.want {
			t.Errorf("stripInlineComment(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
