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
	_, base := SplitConfig(baseConf)

	// Change only work_mem (a reload-safe/user param).
	_, changed := SplitConfig(`# rendered by test
shared_buffers = '128MB'		# (change requires restart)
work_mem = '8MB'
max_wal_size = '1GB'			# sighup
`)

	if changed.RestartHash != base.RestartHash {
		t.Errorf(
			"restart-hash moved on a reload-only change: %s -> %s",
			base.RestartHash,
			changed.RestartHash,
		)
	}
	if changed.ReloadHash == base.ReloadHash {
		t.Errorf("reload-hash did not move on a work_mem change (still %s)", base.ReloadHash)
	}
}

func TestSplitHashesRestartChangeMovesRestartHash(t *testing.T) {
	_, base := SplitConfig(baseConf)

	// Change shared_buffers (a postmaster/restart param).
	_, changed := SplitConfig(`# rendered by test
shared_buffers = '256MB'		# (change requires restart)
work_mem = '4MB'
max_wal_size = '1GB'			# sighup
`)

	if changed.RestartHash == base.RestartHash {
		t.Errorf(
			"restart-hash did not move on a shared_buffers change (still %s)",
			base.RestartHash,
		)
	}
	if changed.ReloadHash != base.ReloadHash {
		t.Errorf(
			"reload-hash moved on a restart-only change: %s -> %s",
			base.ReloadHash,
			changed.ReloadHash,
		)
	}
}

func TestSplitHashesCosmeticEditsMoveNeither(t *testing.T) {
	_, base := SplitConfig(baseConf)

	// Reordered, differently commented, extra blank lines, different inline
	// comment text — same effective values.
	_, cosmetic := SplitConfig(`
# a different header

max_wal_size = '1GB'   # reload-safe, different comment
work_mem = '4MB'

shared_buffers = '128MB'
`)

	if cosmetic.RestartHash != base.RestartHash {
		t.Errorf(
			"restart-hash moved on a cosmetic-only edit: %s -> %s",
			base.RestartHash,
			cosmetic.RestartHash,
		)
	}
	if cosmetic.ReloadHash != base.ReloadHash {
		t.Errorf(
			"reload-hash moved on a cosmetic-only edit: %s -> %s",
			base.ReloadHash,
			cosmetic.ReloadHash,
		)
	}
}

func TestSplitHashesLastWins(t *testing.T) {
	// A later assignment is the effective value; hashing a config whose final
	// work_mem is 8MB must equal one that sets it to 8MB once.
	_, dup := SplitConfig(`work_mem = '4MB'
work_mem = '8MB'
`)
	_, single := SplitConfig(`work_mem = '8MB'
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

func TestReloadSettings(t *testing.T) {
	rendered := `# rendered
shared_buffers = '128MB'		# postmaster → restart, excluded
work_mem = '32MB'			# user → reload
max_wal_size = 1024MB			# sighup → reload, already unquoted
log_line_prefix = '%h %m [%p] '		# superuser → reload, quoted with special chars
cron.database_name = 'postgres'		# namespaced → restart, excluded
`
	_, split := SplitConfig(rendered)
	got := split.ReloadSettings

	if got["work_mem"] != "32MB" {
		t.Errorf("work_mem = %q, want 32MB (unquoted)", got["work_mem"])
	}
	if got["max_wal_size"] != "1024MB" {
		t.Errorf("max_wal_size = %q, want 1024MB", got["max_wal_size"])
	}
	if got["log_line_prefix"] != "%h %m [%p] " {
		t.Errorf("log_line_prefix = %q, want unquoted verbatim", got["log_line_prefix"])
	}
	if _, ok := got["shared_buffers"]; ok {
		t.Error("shared_buffers (postmaster) must be excluded from reload settings")
	}
	if _, ok := got["cron.database_name"]; ok {
		t.Error("cron.database_name (namespaced→restart) must be excluded")
	}
}
