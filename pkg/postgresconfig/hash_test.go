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
	_, base := StampAndSplit(baseConf)

	// Change only work_mem (a reload-safe/user param).
	_, changed := StampAndSplit(`# rendered by test
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
	_, base := StampAndSplit(baseConf)

	// Change shared_buffers (a postmaster/restart param).
	_, changed := StampAndSplit(`# rendered by test
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
	_, base := StampAndSplit(baseConf)

	// Reordered, differently commented, extra blank lines, different inline
	// comment text — same effective values.
	_, cosmetic := StampAndSplit(`
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
	_, dup := StampAndSplit(`work_mem = '4MB'
work_mem = '8MB'
`)
	_, single := StampAndSplit(`work_mem = '8MB'
`)
	if dup.ReloadHash != single.ReloadHash {
		t.Errorf("last-wins not honored: dup=%s single=%s", dup.ReloadHash, single.ReloadHash)
	}
}

// TestSplitHashesValueFormatting documents how the reload-hash treats value
// formatting. The hash is over the raw file token (with the renderer's quotes
// undone), not PostgreSQL's normalized value:
//
//   - Quoting is transparent: '4MB' and 4MB unquote to the same token, so a
//     quote-only edit does NOT move the hash and does NOT trigger a reload.
//   - Whitespace inside the value IS significant: '4 MB' is a different token
//     from '4MB', so it moves the hash and triggers a reload. PostgreSQL treats
//     the two as equal, so this reload is redundant — but it is a plain SIGHUP
//     with no restart and no dropped connections, so a redundant reload is
//     harmless. Normalizing to PostgreSQL's canonical value would mean
//     reimplementing its unit parser (and would still have to leave string GUCs
//     like log_line_prefix, whose spaces are meaningful, untouched); that is out
//     of scope here.
func TestSplitHashesValueFormatting(t *testing.T) {
	reloadHashOf := func(conf string) string {
		_, split := StampAndSplit(conf)
		return split.ReloadHash
	}

	// Quote-only difference: must NOT change the hash (no spurious reload).
	if q, u := reloadHashOf("work_mem = '4MB'\n"), reloadHashOf("work_mem = 4MB\n"); q != u {
		t.Errorf(
			"quoting moved the reload-hash: '4MB'=%s vs 4MB=%s (a quote-only edit must not reload)",
			q,
			u,
		)
	}

	// Internal whitespace: token-based hashing treats '4 MB' as distinct from
	// '4MB'. This documents the known (harmless) redundant-reload behavior.
	if s, q := reloadHashOf("work_mem = '4 MB'\n"), reloadHashOf("work_mem = '4MB'\n"); s == q {
		t.Errorf("expected '4 MB' to hash differently from '4MB' under token-based comparison")
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

		// Malformed inputs must be handled without panicking. An unterminated
		// quote leaves the parser "inside" the string, so any later '#' is treated
		// as part of the value and the whole line is returned unchanged.
		{"'4MB", "'4MB"},
		{"'4MB # comment and no trailing quote", "'4MB # comment and no trailing quote"},
		// A properly closed quote still strips the trailing comment; the stray ''
		// after the '#' is never reached.
		{"'4MB' # comment with '' inside", "'4MB' "},
		// Trailing escaped '' keeps the parser inside the (unterminated) string.
		{"'just a test ''", "'just a test ''"},
	}
	for _, tt := range tests {
		if got := stripInlineComment(tt.in); got != tt.want {
			t.Errorf("stripInlineComment(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestSplitMalformedValuesNoPanic feeds a rendered config whose values are
// malformed (unterminated quotes, stray doubled quotes) through the full
// parse+split path. The point is robustness: a bad value must never panic or
// wedge the reconcile — it is parsed to some deterministic token and hashed like
// any other, and PostgreSQL is left to reject it at reload (surfaced as a
// not-applied mismatch, never as a false "current").
func TestSplitMalformedValuesNoPanic(t *testing.T) {
	const malformed = `work_mem = '4MB
maintenance_work_mem = '2MB # comment and no trailing quote
random_page_cost = 'just a test ''
log_line_prefix = '%h %m [%p] '		# ok, for contrast
`
	// Deterministic: the same malformed input always hashes the same way.
	r1, s1 := StampAndSplit(malformed)
	r2, s2 := StampAndSplit(malformed)
	if s1.ReloadHash != s2.ReloadHash || s1.RestartHash != s2.RestartHash || r1 != r2 {
		t.Errorf("split of malformed config is not deterministic")
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
	_, split := StampAndSplit(rendered)
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
