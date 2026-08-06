package postgresconfig

import "testing"

func TestApplyResourceSizing_Memory(t *testing.T) {
	cfg := Defaults()
	// 512Mi memory, no CPU, no disk.
	if err := ApplyResourceSizing(&cfg, 512*mib, 0, 0); err != nil {
		t.Fatalf("ApplyResourceSizing() error = %v", err)
	}
	checks := map[string]string{
		"SharedBuffers":      "128MB", // 512Mi / 4
		"EffectiveCacheSize": "384MB", // 512Mi * 3/4
		"MaintenanceWorkMem": "32MB",  // 512Mi / 16
		"WorkMem":            "2184kB",
		"WalBuffers":         "3932kB",
	}
	got := map[string]string{
		"SharedBuffers":      cfg.SharedBuffers,
		"EffectiveCacheSize": cfg.EffectiveCacheSize,
		"MaintenanceWorkMem": cfg.MaintenanceWorkMem,
		"WorkMem":            cfg.WorkMem,
		"WalBuffers":         cfg.WalBuffers,
	}
	for k, want := range checks {
		if got[k] != want {
			t.Errorf("%s = %q, want %q", k, got[k], want)
		}
	}
}

func TestApplyResourceSizing_MaintenanceWorkMemCap(t *testing.T) {
	cfg := Defaults()
	// 64Gi / 16 = 4Gi, which must be capped at 2GB.
	if err := ApplyResourceSizing(&cfg, 64*gib, 0, 0); err != nil {
		t.Fatalf("ApplyResourceSizing() error = %v", err)
	}
	if cfg.MaintenanceWorkMem != "2GB" {
		t.Errorf("MaintenanceWorkMem = %q, want 2GB (capped)", cfg.MaintenanceWorkMem)
	}
}

func TestApplyResourceSizing_CPU(t *testing.T) {
	tests := map[string]struct {
		millicores int64
		wantWorker int // MaxWorkerProcesses; 0 means "unchanged from baseline"
		wantGather int
		wantMaint  int
	}{
		"below threshold keeps baseline": {
			millicores: 3999,
			wantWorker: 6,
			wantGather: 1,
			wantMaint:  1,
		},
		"4 cores": {
			millicores: 4000,
			wantWorker: 4,
			wantGather: 2,
			wantMaint:  2,
		},
		"8 cores caps maintenance at 4": {
			millicores: 8000,
			wantWorker: 8,
			wantGather: 4,
			wantMaint:  4,
		},
		"16 cores caps maintenance at 4": {
			millicores: 16000,
			wantWorker: 16,
			wantGather: 8,
			wantMaint:  4,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := Defaults()
			if err := ApplyResourceSizing(&cfg, 0, tc.millicores, 0); err != nil {
				t.Fatalf("ApplyResourceSizing() error = %v", err)
			}
			if cfg.MaxWorkerProcesses != tc.wantWorker {
				t.Errorf("MaxWorkerProcesses = %d, want %d", cfg.MaxWorkerProcesses, tc.wantWorker)
			}
			if cfg.MaxParallelWorkersPerGather != tc.wantGather {
				t.Errorf(
					"MaxParallelWorkersPerGather = %d, want %d",
					cfg.MaxParallelWorkersPerGather,
					tc.wantGather,
				)
			}
			if cfg.MaxParallelMaintenanceWorkers != tc.wantMaint {
				t.Errorf(
					"MaxParallelMaintenanceWorkers = %d, want %d",
					cfg.MaxParallelMaintenanceWorkers,
					tc.wantMaint,
				)
			}
		})
	}
}

func TestApplyResourceSizing_WorkMemUsesParallelWorkers(t *testing.T) {
	// With >=4 cores, max_parallel_workers_per_gather rises, which divides
	// work_mem down relative to the single-worker case.
	single := Defaults()
	_ = ApplyResourceSizing(&single, 512*mib, 0, 0)
	parallel := Defaults()
	_ = ApplyResourceSizing(&parallel, 512*mib, 8000, 0)
	if single.WorkMem == parallel.WorkMem {
		t.Errorf("work_mem should shrink with more parallel workers: both %q", single.WorkMem)
	}
}

func TestApplyResourceSizing_WAL(t *testing.T) {
	cfg := Defaults()
	if err := ApplyResourceSizing(&cfg, 0, 0, 1*gib); err != nil {
		t.Fatalf("ApplyResourceSizing() error = %v", err)
	}
	checks := map[string]string{
		"MinWalSize":         "64MB",
		"MaxWalSize":         "256MB",
		"WalKeepSize":        "128MB",
		"MaxSlotWalKeepSize": "256MB",
	}
	got := map[string]string{
		"MinWalSize":         cfg.MinWalSize,
		"MaxWalSize":         cfg.MaxWalSize,
		"WalKeepSize":        cfg.WalKeepSize,
		"MaxSlotWalKeepSize": cfg.MaxSlotWalKeepSize,
	}
	for k, want := range checks {
		if got[k] != want {
			t.Errorf("%s = %q, want %q", k, got[k], want)
		}
	}
}

func TestApplyResourceSizing_WALScalesDownOnSmallVolume(t *testing.T) {
	small := Defaults()
	_ = ApplyResourceSizing(&small, 0, 0, 256*mib)
	// 256Mi volume: max_wal_size = clamp(256/4=64, floor 64, cap) = 64MB, well
	// below the 1Gi volume's 256MB.
	if small.MaxWalSize != "64MB" {
		t.Errorf("MaxWalSize for 256Mi volume = %q, want 64MB", small.MaxWalSize)
	}
}

func TestApplyResourceSizing_ZeroInputsLeaveBaseline(t *testing.T) {
	cfg := Defaults()
	base := Defaults()
	if err := ApplyResourceSizing(&cfg, 0, 0, 0); err != nil {
		t.Fatalf("ApplyResourceSizing() error = %v", err)
	}
	if cfg != base {
		t.Errorf("zero inputs mutated the config: %+v != %+v", cfg, base)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := map[int64]string{
		2 * gib:   "2GB",
		128 * mib: "128MB",
		64 * kib:  "64kB",
		1500:      "1kB", // sub-kB remainder truncated
		0:         "0kB",
	}
	for in, want := range tests {
		if got := formatBytes(in); got != want {
			t.Errorf("formatBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestDeriveWalSettings_Errors(t *testing.T) {
	if _, err := deriveWalSettings(1*uint64(gib), 0); err == nil {
		t.Error("expected error for zero WAL segment size")
	}
	if _, err := deriveWalSettings(1*uint64(gib), megabyte+1); err == nil {
		t.Error("expected error for non-MB-aligned WAL segment size")
	}
	// A WAL segment large enough that its floor exceeds the max_wal_size cap.
	if _, err := deriveWalSettings(1*uint64(gib), 2048*megabyte); err == nil {
		t.Error("expected error when segment size forces max_wal_size above the cap")
	}
}
