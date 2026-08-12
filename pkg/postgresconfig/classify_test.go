package postgresconfig

import "testing"

func TestRequiresRestart(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		// postmaster context → restart
		{"shared_buffers", true},
		{"max_connections", true},
		{"wal_level", true},
		{"max_worker_processes", true},
		// internal context → restart
		{"block_size", true},
		{"wal_segment_size", true},
		// sighup context → reload
		{"max_wal_size", false},
		{"autovacuum", false},
		{"checkpoint_completion_target", false},
		// user context → reload
		{"work_mem", false},
		{"maintenance_work_mem", false},
		{"effective_cache_size", false},
		{"random_page_cost", false},
		// superuser context → reload
		{"session_preload_libraries", false},
		// case-insensitive
		{"Work_Mem", false},
		{"SHARED_BUFFERS", true},
		// unknown → restart (conservative)
		{"totally_made_up_guc", true},
		// namespaced extension params → restart (conservative)
		{"cron.database_name", true},
		{"auto_explain.log_min_duration", true},
	}
	for _, tt := range tests {
		if got := RequiresRestart(tt.name); got != tt.want {
			t.Errorf("RequiresRestart(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// TestRequiresRestartKnownRestartKeys pins the parameters known to require a
// restart (postmaster context in pg_settings). Most are verified from the
// catalog's real context; cron.log_statement is a pg_cron GUC (PGC_POSTMASTER)
// that is namespaced and absent from the built-in catalog, so it relies on the
// conservative namespaced default. This guards against a catalog regeneration
// that drops the multi-line guc_tables.c entries (several of these wrap) and
// would otherwise silently reclassify them.
func TestRequiresRestartKnownRestartKeys(t *testing.T) {
	restartKeys := []string{
		"max_connections",
		"track_activity_query_size",
		"max_worker_processes",
		"max_locks_per_transaction",
		"max_wal_senders",
		"max_replication_slots",
		"shared_buffers",
		"track_commit_timestamp",
		"max_logical_replication_workers",
		"cron.log_statement", // pg_cron PGC_POSTMASTER; namespaced → conservative restart
	}
	for _, k := range restartKeys {
		if !RequiresRestart(k) {
			t.Errorf("RequiresRestart(%q) = false, want true (restart-required)", k)
		}
	}
}

// TestRequiresRestartMatchesCatalogContext guards the classifier against a
// catalog regenerated without the context column: if every entry parsed with an
// empty context, everything would (conservatively) require a restart, silently
// disabling the reload path. Assert a healthy split exists.
func TestRequiresRestartMatchesCatalogContext(t *testing.T) {
	var reloadable, total int
	for name := range catalog {
		total++
		if !RequiresRestart(name) {
			reloadable++
		}
	}
	if total < 300 {
		t.Fatalf("catalog has %d entries, expected the full PG17 set (389)", total)
	}
	// The majority of PG17 GUCs are reload-safe (sighup/user/superuser); if the
	// context column were missing this would be ~0.
	if reloadable < 200 {
		t.Errorf("only %d/%d params classified reloadable; context column likely missing from catalog", reloadable, total)
	}
}
