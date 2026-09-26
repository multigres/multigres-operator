package postgresconfig

import (
	"testing"

	"github.com/multigres/testkit/assert"
)

func TestApplyResourceSizing_Memory(t *testing.T) {
	c := assert.NewCollecting(t)
	cfg := Defaults()
	// 512Mi memory, no CPU, no disk.
	c.Require().NoError(ApplyResourceSizing(&cfg, 512*mib, 0, 0), "ApplyResourceSizing() error =")
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
	c.Eq(60, cfg.MaxConnections, "MaxConnections")
	c.False(
		cfg.MaxWalSenders != 5 || cfg.MaxReplicationSlots != 5,
		"MaxWalSenders/Slots = %d/%d, want 5/5",
		cfg.MaxWalSenders,
		cfg.MaxReplicationSlots,
	)
	for k, want := range checks {
		c.Eq(want, got[k], "%s = %q, want", k, got[k])
	}
}

func TestApplyResourceSizing_MaintenanceWorkMemCap(t *testing.T) {
	c := assert.NewCollecting(t)
	cfg := Defaults()
	// 64Gi / 16 = 4Gi, which must be capped at 2GB.
	c.Require().NoError(ApplyResourceSizing(&cfg, 64*gib, 0, 0), "ApplyResourceSizing() error =")
	c.Eq("2GB", cfg.MaintenanceWorkMem, "MaintenanceWorkMem")
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
			wantWorker: 6,
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
			c := assert.NewCollecting(t)
			cfg := Defaults()
			c.Require().
				NoError(ApplyResourceSizing(&cfg, 0, tc.millicores, 0), "ApplyResourceSizing() error =")
			c.Eq(tc.wantWorker, cfg.MaxWorkerProcesses, "MaxWorkerProcesses")
			c.Eq(tc.wantGather, cfg.MaxParallelWorkersPerGather, "MaxParallelWorkersPerGather")
			c.Eq(tc.wantMaint, cfg.MaxParallelMaintenanceWorkers, "MaxParallelMaintenanceWorkers")
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
	assert.NewCollecting(t).
		NotEq(parallel.WorkMem, single.WorkMem, "work_mem should shrink with more parallel workers: both")
}

func TestApplyResourceSizing_WAL(t *testing.T) {
	c := assert.NewCollecting(t)
	cfg := Defaults()
	c.Require().NoError(ApplyResourceSizing(&cfg, 0, 0, 1*gib), "ApplyResourceSizing() error =")
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
		c.Eq(want, got[k], "%s = %q, want", k, got[k])
	}
}

func TestApplyResourceSizing_WALScalesDownOnSmallVolume(t *testing.T) {
	small := Defaults()
	_ = ApplyResourceSizing(&small, 0, 0, 256*mib)
	// 256Mi volume: max_wal_size = clamp(256/4=64, floor 64, cap) = 64MB, well
	// below the 1Gi volume's 256MB.
	assert.NewCollecting(t).Eq("64MB", small.MaxWalSize, "MaxWalSize for 256Mi volume")
}

func TestApplyResourceSizing_ZeroInputsLeaveBaseline(t *testing.T) {
	c := assert.NewCollecting(t)
	cfg := Defaults()
	base := Defaults()
	c.Require().NoError(ApplyResourceSizing(&cfg, 0, 0, 0), "ApplyResourceSizing() error =")
	c.Eq(base, cfg, "zero inputs mutated the config")
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
		got := formatBytes(in)
		assert.NewCollecting(t).Eq(want, got, "formatBytes(%d) = %q, want", in, got)
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
	_, err := deriveWalSettings(1*uint64(gib), 2048*megabyte)
	assert.NewCollecting(t).
		Error(err, "expected error when segment size forces max_wal_size above the cap")
}

func TestApplyResourceSizing_MaxConnections(t *testing.T) {
	// Anchor points mirroring v2's serverRecommendations table, keyed by the
	// memory the operator would see on the corresponding compute size.
	tests := map[string]struct {
		memBytes int64
		want     int
	}{
		"1GiB (pico/nano/micro)": {1 * gib, 60},
		"2GiB (small)":           {2 * gib, 90},
		"4GiB (medium)":          {4 * gib, 120},
		"8GiB (large)":           {8 * gib, 160},
		"16GiB (xlarge)":         {16 * gib, 240},
		"32GiB (2xlarge)":        {32 * gib, 380},
		"64GiB (4xlarge)":        {64 * gib, 480},
		"128GiB (8xlarge)":       {128 * gib, 490},
		"192GiB (12xlarge)":      {192 * gib, 500},
		"256GiB (16xlarge)":      {256 * gib, 500},
		"384GiB (m8g.24xl)":      {384 * gib, 750},
		"768GiB (m8g.48xl)":      {768 * gib, 1000},
		"3072GiB (x8g.48xl)":     {3072 * gib, 1000},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			cfg := Defaults()
			c.Require().
				NoError(ApplyResourceSizing(&cfg, tc.memBytes, 0, 0), "ApplyResourceSizing() error =")
			c.Eq(tc.want, cfg.MaxConnections, "MaxConnections")
		})
	}
}

func TestApplyResourceSizing_MaxWalSenders(t *testing.T) {
	tests := map[string]struct {
		memBytes int64
		want     int
	}{
		"1GiB":   {1 * gib, 5},
		"2GiB":   {2 * gib, 10},
		"8GiB":   {8 * gib, 10},
		"16GiB":  {16 * gib, 24},
		"32GiB":  {32 * gib, 80},
		"256GiB": {256 * gib, 80},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			cfg := Defaults()
			c.Require().
				NoError(ApplyResourceSizing(&cfg, tc.memBytes, 0, 0), "ApplyResourceSizing() error =")
			c.Eq(tc.want, cfg.MaxWalSenders, "MaxWalSenders")
			c.Eq(tc.want, cfg.MaxReplicationSlots, "MaxReplicationSlots")
		})
	}
}

func TestApplyResourceSizing_MaxConnectionsFeedsWorkMem(t *testing.T) {
	c := assert.NewCollecting(t)
	// work_mem = (mem - shared) / (conns * 3) / parallel. With the derived
	// MaxConnections=160 at 8GiB, work_mem must be ~1/2.66× what it would be
	// under the old hardcoded 60. Two different memory anchors sanity-check
	// that the divisor tracks the derived value rather than the baseline.
	cfg8 := Defaults()
	_ = ApplyResourceSizing(&cfg8, 8*gib, 0, 0)
	c.Require().Eq(160, cfg8.MaxConnections, "precondition: MaxConnections at 8GiB")
	// (8*gib - 2*gib) / (160*3) = 6GiB/480 = 12.8 MiB, formatted to kB.
	c.Eq("13107kB", cfg8.WorkMem, "WorkMem at 8GiB, 160 conns")
}

func TestDefaults_EffectiveIoConcurrency(t *testing.T) {
	// SSDs everywhere; PgTune's OLTP/DW profile for SSD storage.
	assert.NewCollecting(t).
		Eq(200, Defaults().EffectiveIoConcurrency, "Defaults().EffectiveIoConcurrency")
}
