// Package backuphandler holds what the backup controllers share: metrics,
// labels and the per-shard queue rules.
package backuphandler

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// ScheduleLastSuccess is the completion time of the newest successful
	// attempt per schedule and shard.
	ScheduleLastSuccess = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_backup_schedule_last_success_timestamp_seconds",
			Help: "Completion time of the newest successful scheduled backup attempt.",
		},
		[]string{"namespace", "cluster", "database", "tablegroup", "shard", "schedule"},
	)
	// ScheduleRunsMissed counts runs that could not start within
	// startingDeadlineSeconds.
	ScheduleRunsMissed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "multigres_backup_schedule_runs_missed_total",
			Help: "Scheduled backup runs missed because they could not start in time.",
		},
		[]string{"namespace", "schedule"},
	)
	// ScheduleRunsSkipped counts runs skipped because an unfinished backup of
	// the same or a stronger type already covered the window.
	ScheduleRunsSkipped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "multigres_backup_schedule_runs_skipped_total",
			Help: "Scheduled backup runs skipped because an equal or stronger backup was unfinished.",
		},
		[]string{"namespace", "schedule"},
	)
	// AttemptsTotal counts finished attempts.
	AttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "multigres_backup_attempts_total",
			Help: "Finished MultigresBackup attempts.",
		},
		[]string{"result", "reason", "type", "trigger"},
	)
	// AttemptDuration is the time from Running to finished.
	AttemptDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "multigres_backup_attempt_duration_seconds",
			Help:    "Time from multiadmin accepting a backup to the attempt finishing.",
			Buckets: prometheus.ExponentialBuckets(30, 2, 12), // 30s .. ~17h
		},
	)
	// PendingDuration is the time an attempt waited in Pending.
	PendingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "multigres_backup_pending_seconds",
			Help:    "Time a MultigresBackup waited in the per-shard queue before triggering.",
			Buckets: prometheus.ExponentialBuckets(1, 3, 12), // 1s .. ~2d
		},
	)
)

func init() {
	metrics.Registry.MustRegister(
		ScheduleLastSuccess,
		ScheduleRunsMissed,
		ScheduleRunsSkipped,
		AttemptsTotal,
		AttemptDuration,
		PendingDuration,
	)
}
