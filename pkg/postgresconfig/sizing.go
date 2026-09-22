package postgresconfig

import "fmt"

const (
	kib = int64(1) << 10
	mib = int64(1) << 20
	gib = int64(1) << 30

	// maintenanceWorkMemCapBytes caps maintenance_work_mem at 2GB.
	maintenanceWorkMemCapBytes = 2 * gib
)

// ApplyResourceSizing overrides the size-sensitive fields of cfg from resolved
// per-shard resource inputs, mutating cfg in place:
//
//   - memory (bytes) and CPU (millicores) drive the memory/worker knobs
//     (shared_buffers, effective_cache_size, maintenance_work_mem, work_mem,
//     wal_buffers, and the parallel-worker settings);
//   - disk (bytes) drives the WAL disk knobs.
//
// Any input that is zero leaves the corresponding baseline defaults untouched,
// so a shard with unset resources keeps the shipped baseline.
func ApplyResourceSizing(cfg *Config, memBytes, cpuMillicores, diskBytes int64) error {
	// CPU first: it sets MaxParallelWorkersPerGather, which work_mem divides by.
	if cpuMillicores > 0 {
		cores := int(cpuMillicores / 1000)
		cfg.MaxWorkerProcesses = max(cores, cfg.MaxWorkerProcesses)
		cfg.MaxParallelWorkers = max(cores, cfg.MaxParallelWorkers)
		cfg.MaxParallelWorkersPerGather = max(cores/2, cfg.MaxParallelWorkersPerGather)
		cfg.MaxParallelMaintenanceWorkers = min(max(cores/2, cfg.MaxParallelMaintenanceWorkers), 4)
	}

	if memBytes > 0 {
		// Derive MaxConnections before WorkMem: WorkMem divides by conns.
		cfg.MaxConnections = deriveMaxConnections(memBytes)
		cfg.MaxWalSenders = deriveWalSenders(memBytes)
		cfg.MaxReplicationSlots = cfg.MaxWalSenders

		shared := memBytes / 4
		cfg.SharedBuffers = formatBytes(shared)
		cfg.EffectiveCacheSize = formatBytes(memBytes * 3 / 4)
		cfg.MaintenanceWorkMem = formatBytes(min(memBytes/16, maintenanceWorkMemCapBytes))
		cfg.WalBuffers = formatBytes(clampInt64(shared*3/100, 32*kib, 16*mib))

		parallel := int64(max(cfg.MaxParallelWorkersPerGather, 1))
		conns := int64(max(cfg.MaxConnections, 1))
		cfg.WorkMem = formatBytes(max((memBytes-shared)/(conns*3)/parallel, 64*kib))
	}

	if diskBytes > 0 {
		ws, err := deriveWalSettings(uint64(diskBytes), defaultWalSegmentSizeBytes)
		if err != nil {
			return err
		}
		cfg.MinWalSize = fmt.Sprintf("%dMB", ws.minWalSizeMB)
		cfg.MaxWalSize = fmt.Sprintf("%dMB", ws.maxWalSizeMB)
		cfg.WalKeepSize = fmt.Sprintf("%dMB", ws.walKeepSizeMB)
		cfg.MaxSlotWalKeepSize = fmt.Sprintf("%dMB", ws.maxSlotWalKeepSizeMB)
	}
	return nil
}

// formatBytes renders a byte count as a postgresql.conf size string using the
// largest unit (GB/MB/kB) that divides it evenly. PostgreSQL's smallest memory
// unit is kB, so sub-kB remainders are truncated.
func formatBytes(b int64) string {
	kb := b / kib
	if kb <= 0 {
		return "0kB"
	}
	switch {
	case kb%(kib*kib) == 0:
		return fmt.Sprintf("%dGB", kb/(kib*kib))
	case kb%kib == 0:
		return fmt.Sprintf("%dMB", kb/kib)
	default:
		return fmt.Sprintf("%dkB", kb)
	}
}

func clampInt64(v, lo, hi int64) int64 {
	return min(max(v, lo), hi)
}

func deriveMaxConnections(memBytes int64) int {
	memGiB := memBytes / gib
	switch {
	case memGiB < 2:
		return 60
	case memGiB < 4:
		return 90
	case memGiB < 8:
		return 120
	case memGiB < 16:
		return 160
	case memGiB < 32:
		return 240
	case memGiB < 64:
		return 380
	case memGiB < 128:
		return 480
	case memGiB < 192:
		return 490
	case memGiB < 384:
		return 500
	case memGiB < 768:
		return 750
	default:
		return 1000
	}
}

func deriveWalSenders(memBytes int64) int {
	memGiB := memBytes / gib
	switch {
	case memGiB < 2:
		return 5
	case memGiB < 16:
		return 10
	case memGiB < 32:
		return 24
	default:
		return 80
	}
}

// ---------------------------------------------------------------------------
// WAL disk-usage sizing: scale PostgreSQL's WAL disk knobs to the data volume
// so routine WAL retention is budgeted below the disk size. The upper clamps
// keep sane values on large volumes; smaller volumes scale down proportionally.
// PostgreSQL requires min_wal_size and max_wal_size to be at least two WAL
// segments, so the lower clamps also account for the segment size.
// ---------------------------------------------------------------------------

const (
	megabyte                   = uint64(1 << 20)
	defaultWalSegmentSizeBytes = 16 * megabyte
	maxWalSizeCapMB            = uint64(4096)
)

// walSettings holds the WAL disk-usage settings derived from the size of the
// volume backing the data directory, in megabytes.
type walSettings struct {
	minWalSizeMB         uint64
	maxWalSizeMB         uint64
	walKeepSizeMB        uint64
	maxSlotWalKeepSizeMB uint64
}

func deriveWalSettings(volumeBytes, walSegmentSizeBytes uint64) (walSettings, error) {
	if walSegmentSizeBytes == 0 || walSegmentSizeBytes%megabyte != 0 {
		return walSettings{}, fmt.Errorf(
			"WAL segment size must be a non-zero whole number of megabytes, got %d bytes",
			walSegmentSizeBytes,
		)
	}

	volMB := volumeBytes / megabyte
	walSegmentSizeMB := walSegmentSizeBytes / megabyte
	// Keep enough distance between the two PostgreSQL minimums that min_wal_size
	// does not consume the entire max_wal_size allowance.
	maxWalFloor := max(uint64(64), 4*walSegmentSizeMB)
	if maxWalFloor > maxWalSizeCapMB {
		return walSettings{}, fmt.Errorf(
			"WAL segment size %dMB requires max_wal_size above the supported %dMB cap",
			walSegmentSizeMB, maxWalSizeCapMB,
		)
	}
	minWalFloor := max(uint64(32), 2*walSegmentSizeMB)
	maxWal := clampUint64(volMB/4, maxWalFloor, maxWalSizeCapMB)

	return walSettings{
		minWalSizeMB: clampUint64(maxWal/4, minWalFloor, max(uint64(1024), minWalFloor)),
		maxWalSizeMB: maxWal,
		walKeepSizeMB: clampUint64(
			volMB/8,
			max(uint64(32), walSegmentSizeMB),
			max(uint64(1000), walSegmentSizeMB),
		),
		maxSlotWalKeepSizeMB: maxWal,
	}, nil
}

func clampUint64(v, lo, hi uint64) uint64 {
	return min(max(v, lo), hi)
}
