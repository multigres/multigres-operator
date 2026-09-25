package observer

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/jackc/pgx/v5"
	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/tools/observer/pkg/common"
	"github.com/multigres/multigres-operator/tools/observer/pkg/report"
)

func (o *Observer) checkReplication(ctx context.Context) {
	if !o.enableSQLProbe {
		return
	}

	var shards multigresv1alpha1.ShardList
	if err := o.client.List(ctx, &shards, o.listOpts()...); err != nil {
		return
	}

	replData := make([]map[string]any, 0, len(shards.Items))
	for i := range shards.Items {
		shard := &shards.Items[i]
		if shard.DeletionTimestamp != nil || shard.Status.PodRoles == nil {
			continue
		}
		o.checkShardReplication(ctx, shard)

		var primaryCount, replicaCount, quarantinedCount int
		for _, role := range shard.Status.PodRoles {
			switch role {
			case "PRIMARY", "primary":
				primaryCount++
			case "QUARANTINED":
				quarantinedCount++
			default:
				replicaCount++
			}
		}
		replData = append(replData, map[string]any{
			"shard":            shard.Name,
			"namespace":        shard.Namespace,
			"primaryCount":     primaryCount,
			"replicaCount":     replicaCount,
			"quarantinedCount": quarantinedCount,
			"podRoles":         shard.Status.PodRoles,
		})
	}

	if o.probes != nil {
		o.probes.Set("replication", map[string]any{
			"shards":          replData,
			"sqlProbeEnabled": o.enableSQLProbe,
		})
	}
}

func (o *Observer) checkShardReplication(ctx context.Context, shard *multigresv1alpha1.Shard) {
	comp := fmt.Sprintf("shard/%s/%s", shard.Namespace, shard.Name)
	shardLabelValue := shard.Labels[common.LabelMultigresShard]
	password := o.fetchShardPassword(ctx, shard)

	// Classify pods by role. QUARANTINED pods are kept out of the replica set:
	// their postgres cannot start, so they neither stream from the primary nor
	// count toward the expected replica connection count. Counting them as
	// replicas would inflate expectedReplicas and produce false "primary has
	// fewer standbys than expected" findings. They are surfaced separately and
	// replaced by quarantine remediation (data PVC wipe + re-bootstrap).
	var primaryPodNames []string
	var replicaPodNames []string
	var quarantinedPodNames []string
	for podName, role := range shard.Status.PodRoles {
		switch role {
		case "PRIMARY", "primary":
			primaryPodNames = append(primaryPodNames, podName)
		case "QUARANTINED":
			quarantinedPodNames = append(quarantinedPodNames, podName)
		default:
			replicaPodNames = append(replicaPodNames, podName)
		}
	}

	if len(quarantinedPodNames) > 0 {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "replication",
			Component: comp,
			Message: fmt.Sprintf(
				"Shard has %d QUARANTINED pod(s) awaiting remediation (data PVC wipe + re-bootstrap): %v",
				len(quarantinedPodNames), quarantinedPodNames,
			),
			Details: map[string]any{
				"quarantinedPods": quarantinedPodNames,
			},
		})
	}

	if len(primaryPodNames) == 0 {
		// CRD says no primaries — run SQL probes to detect stale podRoles.
		o.detectStalePodRoles(ctx, shard, shardLabelValue, comp, password)
		return
	}

	// Get pool pods to look up IPs and pool membership.
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		o.listOpts(client.MatchingLabels{
			common.LabelMultigresShard: shardLabelValue,
			common.LabelAppComponent:   common.ComponentPool,
		})...,
	); err != nil {
		return
	}

	podIPs := make(map[string]string, len(pods.Items))
	podPool := make(map[string]string, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		podPool[p.Name] = p.Labels[common.LabelMultigresPool]
		if p.Status.PodIP != "" && p.Status.Phase == corev1.PodRunning &&
			p.DeletionTimestamp == nil {
			if o.isPodInGracePeriod(p.Name) {
				continue
			}
			podIPs[p.Name] = p.Status.PodIP
		}
	}

	// Count replicas per pool so each primary is checked against its own pool's replicas.
	replicasPerPool := make(map[string]int)
	for _, name := range replicaPodNames {
		replicasPerPool[podPool[name]]++
	}

	// Check primaries: pg_stat_replication, sync state, replication lag, write probe.
	for _, primaryName := range primaryPodNames {
		ip, ok := podIPs[primaryName]
		if !ok {
			continue
		}
		expectedReplicas := replicasPerPool[podPool[primaryName]]
		o.probePrimaryReplication(ctx, ip, primaryName, comp, expectedReplicas, password)
	}

	// Check replicas: WAL receiver status, replay paused state.
	for _, replicaName := range replicaPodNames {
		ip, ok := podIPs[replicaName]
		if !ok {
			continue
		}
		o.probeReplicaHealth(ctx, ip, replicaName, comp, password)
	}

	// Split-brain detection: verify pg_is_in_recovery() matches podRoles.
	o.detectSplitBrain(ctx, podIPs, shard.Status.PodRoles, comp, password)
}

// detectStalePodRoles runs SQL probes on all pool pods when the CRD reports 0
// primaries. If any pod reports pg_is_in_recovery()=false (i.e. is actually a
// primary), the CRD's podRoles are stale and a finding is emitted.
func (o *Observer) detectStalePodRoles(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	shardLabel, comp, password string,
) {
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		o.listOpts(client.MatchingLabels{
			common.LabelMultigresShard: shardLabel,
			common.LabelAppComponent:   common.ComponentPool,
		})...,
	); err != nil {
		return
	}

	var sqlPrimaries []string
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.PodIP == "" || p.Status.Phase != corev1.PodRunning ||
			p.DeletionTimestamp != nil || o.isPodInGracePeriod(p.Name) {
			continue
		}

		conn, err := o.connectPostgres(ctx, p.Status.PodIP, password)
		if err != nil {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, common.ConnectivityTimeout)
		var inRecovery bool
		err = conn.QueryRow(probeCtx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
		cancel()
		_ = conn.Close(ctx)
		if err != nil {
			continue
		}
		if !inRecovery {
			sqlPrimaries = append(sqlPrimaries, p.Name)
		}
	}

	if len(sqlPrimaries) > 0 {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "replication",
			Component: comp,
			Message: fmt.Sprintf(
				"CRD podRoles has 0 primaries but SQL confirms %d primary: %v (stale podRoles)",
				len(sqlPrimaries), sqlPrimaries,
			),
			Details: map[string]any{
				"sqlPrimaries": sqlPrimaries,
				"podRoles":     shard.Status.PodRoles,
			},
		})
	}
}

// probePrimaryReplication connects to a primary pod and checks replication topology.
func (o *Observer) probePrimaryReplication(
	ctx context.Context,
	podIP, podName, comp string,
	expectedReplicas int,
	password string,
) {
	conn, err := o.connectPostgres(ctx, podIP, password)
	if err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "replication",
			Component: comp,
			Message:   fmt.Sprintf("Failed to connect to primary %s postgres: %v", podName, err),
		})
		return
	}
	defer func() { _ = conn.Close(ctx) }()

	probeCtx, cancel := context.WithTimeout(ctx, common.ConnectivityTimeout)
	defer cancel()

	// Check if synchronous replication is configured.
	var syncStandbyNames string
	if err := conn.QueryRow(probeCtx, "SHOW synchronous_standby_names").
		Scan(&syncStandbyNames); err != nil {
		o.logger.Debug("failed to query synchronous_standby_names", "pod", podName, "error", err)
		return
	}
	syncConfigured := syncStandbyNames != ""

	// Query replication state with lag info.
	rows, err := conn.Query(probeCtx, `
		SELECT application_name, sync_state, sync_priority,
			COALESCE(EXTRACT(EPOCH FROM replay_lag)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM write_lag)::bigint, 0),
			COALESCE(pg_wal_lsn_diff(sent_lsn, replay_lsn)::bigint, 0)
		FROM pg_stat_replication`)
	if err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "replication",
			Component: comp,
			Message:   fmt.Sprintf("Failed to query pg_stat_replication on %s: %v", podName, err),
		})
		return
	}
	defer rows.Close()

	type standbyInfo struct {
		appName       string
		syncState     string
		syncPriority  int
		replayLagSecs int64
		writeLagSecs  int64
		lagBytes      int64
	}

	var standbys []standbyInfo
	for rows.Next() {
		var s standbyInfo
		if err := rows.Scan(
			&s.appName,
			&s.syncState,
			&s.syncPriority,
			&s.replayLagSecs,
			&s.writeLagSecs,
			&s.lagBytes,
		); err != nil {
			continue
		}
		standbys = append(standbys, s)
	}

	if len(standbys) == 0 && expectedReplicas > 0 {
		o.reporter.Report(report.Finding{
			Severity:  o.effectiveSeverity(podName, report.SeverityError),
			Check:     "replication",
			Component: comp,
			Message: fmt.Sprintf(
				"Primary %s has 0 replication connections but %d replicas expected",
				podName,
				expectedReplicas,
			),
			Details: map[string]any{"pod": podName, "expectedReplicas": expectedReplicas},
		})
		return
	}

	for _, s := range standbys {
		// Truncated application_name detection.
		if len(s.appName) == common.PGNameDataLen {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityFatal,
				Check:     "replication",
				Component: comp,
				Message: fmt.Sprintf(
					"Standby application_name is exactly %d chars (likely truncated by PostgreSQL NAMEDATALEN): %q",
					common.PGNameDataLen,
					s.appName,
				),
				Details: map[string]any{
					"pod":             podName,
					"applicationName": s.appName,
					"nameLength":      len(s.appName),
				},
			})
		}

		// Async standbys when sync replication is configured.
		if syncConfigured && s.syncState == "async" {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityFatal,
				Check:     "replication",
				Component: comp,
				Message: fmt.Sprintf(
					"Standby %q is async but synchronous replication is configured (%s) — writes will block",
					s.appName,
					syncStandbyNames,
				),
				Details: map[string]any{
					"pod":                     podName,
					"applicationName":         s.appName,
					"syncState":               s.syncState,
					"synchronousStandbyNames": syncStandbyNames,
				},
			})
		}

		// Replication lag detection.
		if s.replayLagSecs > common.ReplicationLagWarnSecs {
			sev := report.SeverityWarn
			if s.replayLagSecs > common.ReplicationLagErrorSecs {
				sev = report.SeverityError
			}
			o.reporter.Report(report.Finding{
				Severity:  sev,
				Check:     "replication",
				Component: comp,
				Message: fmt.Sprintf(
					"Standby %q has %ds replay lag (%d bytes behind)",
					s.appName,
					s.replayLagSecs,
					s.lagBytes,
				),
				Details: map[string]any{
					"pod":             podName,
					"applicationName": s.appName,
					"replayLagSecs":   s.replayLagSecs,
					"writeLagSecs":    s.writeLagSecs,
					"lagBytes":        s.lagBytes,
				},
			})
		}
	}

	// Write probe: detect blocked writes.
	o.probeWrite(ctx, conn, podName, comp)
}

// probeReplicaHealth connects to a replica pod and checks WAL receiver and replay status.
func (o *Observer) probeReplicaHealth(ctx context.Context, podIP, podName, comp, password string) {
	conn, err := o.connectPostgres(ctx, podIP, password)
	if err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "replication",
			Component: comp,
			Message:   fmt.Sprintf("Replica %s postgres unreachable: %v", podName, err),
			Details:   map[string]any{"pod": podName},
		})
		return
	}
	defer func() { _ = conn.Close(ctx) }()

	probeCtx, cancel := context.WithTimeout(ctx, common.ConnectivityTimeout)
	defer cancel()

	// Check WAL receiver status.
	var walReceiverCount int
	if err := conn.QueryRow(probeCtx, "SELECT COUNT(*) FROM pg_stat_wal_receiver").
		Scan(&walReceiverCount); err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "replication",
			Component: comp,
			Message:   fmt.Sprintf("Failed to query WAL receiver on replica %s: %v", podName, err),
			Details:   map[string]any{"pod": podName},
		})
	} else if walReceiverCount == 0 {
		o.reporter.Report(report.Finding{
			Severity:  o.effectiveSeverity(podName, report.SeverityError),
			Check:     "replication",
			Component: comp,
			Message: fmt.Sprintf(
				"Replica %s has no WAL receiver running (not connected to primary)",
				podName,
			),
			Details: map[string]any{"pod": podName},
		})
	} else {
		// Check if the WAL receiver is actually streaming.
		var walStatus string
		if err := conn.QueryRow(probeCtx, "SELECT status FROM pg_stat_wal_receiver LIMIT 1").
			Scan(&walStatus); err == nil {
			if walStatus != "streaming" {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityWarn,
					Check:     "replication",
					Component: comp,
					Message: fmt.Sprintf(
						"Replica %s WAL receiver status is %q (expected streaming)",
						podName,
						walStatus,
					),
					Details: map[string]any{"pod": podName, "walReceiverStatus": walStatus},
				})
			}
		}
	}

	// Check if WAL replay is paused.
	var isReplayPaused bool
	if err := conn.QueryRow(probeCtx, "SELECT pg_is_wal_replay_paused()").
		Scan(&isReplayPaused); err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "replication",
			Component: comp,
			Message: fmt.Sprintf(
				"Failed to query WAL replay status on replica %s: %v",
				podName,
				err,
			),
			Details: map[string]any{"pod": podName},
		})
	} else if isReplayPaused {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "replication",
			Component: comp,
			Message:   fmt.Sprintf("Replica %s has WAL replay paused", podName),
			Details:   map[string]any{"pod": podName},
		})
	}
}

// detectSplitBrain verifies that pg_is_in_recovery() on each pod matches the
// expected role from podRoles. If multiple pods report as primary, that's split-brain.
func (o *Observer) detectSplitBrain(
	ctx context.Context,
	podIPs map[string]string,
	podRoles map[string]string,
	comp, password string,
) {
	var actualPrimaries []string

	for podName, ip := range podIPs {
		conn, err := o.connectPostgres(ctx, ip, password)
		if err != nil {
			continue
		}

		probeCtx, cancel := context.WithTimeout(ctx, common.ConnectivityTimeout)
		var inRecovery bool
		err = conn.QueryRow(probeCtx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
		cancel()
		_ = conn.Close(ctx)

		if err != nil {
			continue
		}

		if !inRecovery {
			actualPrimaries = append(actualPrimaries, podName)
		}

		// Check mismatch with podRoles.
		expectedRole, exists := podRoles[podName]
		if !exists {
			continue
		}
		isPrimaryRole := expectedRole == "PRIMARY" || expectedRole == "primary"

		if !inRecovery && !isPrimaryRole {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityError,
				Check:     "replication",
				Component: comp,
				Message: fmt.Sprintf(
					"Pod %s reports as primary (pg_is_in_recovery()=false) but podRoles says %s",
					podName,
					expectedRole,
				),
				Details: map[string]any{
					"pod":           podName,
					"expectedRole":  expectedRole,
					"actualPrimary": true,
				},
			})
		} else if inRecovery && isPrimaryRole {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityError,
				Check:     "replication",
				Component: comp,
				Message: fmt.Sprintf(
					"Pod %s reports as replica (pg_is_in_recovery()=true) but podRoles says %s",
					podName,
					expectedRole,
				),
				Details: map[string]any{
					"pod":           podName,
					"expectedRole":  expectedRole,
					"actualPrimary": false,
				},
			})
		}
	}

	if len(actualPrimaries) > 1 {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityFatal,
			Check:     "replication",
			Component: comp,
			Message: fmt.Sprintf(
				"SPLIT-BRAIN: %d pods report as primary: %v",
				len(actualPrimaries),
				actualPrimaries,
			),
			Details: map[string]any{
				"primaryPods": actualPrimaries,
				"count":       len(actualPrimaries),
			},
		})
	}
}

// probeWrite runs a minimal write transaction to detect if writes are blocked
// (e.g., due to broken synchronous replication). The transaction is always
// rolled back and the temp table is session-scoped, so nothing persists.
func (o *Observer) probeWrite(ctx context.Context, conn *pgx.Conn, podName, comp string) {
	writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	tx, err := conn.Begin(writeCtx)
	if err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "replication",
			Component: comp,
			Message:   fmt.Sprintf("Write probe: BEGIN failed on %s: %v", podName, err),
		})
		return
	}

	_, err = tx.Exec(writeCtx, "CREATE TEMP TABLE IF NOT EXISTS _observer_write_probe (x int)")
	rollbackErr := tx.Rollback(writeCtx)

	if err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityFatal,
			Check:     "replication",
			Component: comp,
			Message: fmt.Sprintf(
				"Write probe: writes appear blocked on primary %s: %v",
				podName,
				err,
			),
			Details: map[string]any{"pod": podName, "error": err.Error()},
		})
		return
	}

	if rollbackErr != nil {
		o.logger.Debug("write probe rollback failed", "pod", podName, "error", rollbackErr)
	}
}

// connectPostgres establishes a connection to postgres on a pod IP.
func (o *Observer) connectPostgres(ctx context.Context, podIP, password string) (*pgx.Conn, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=postgres dbname=postgres connect_timeout=5 sslmode=disable",
		podIP,
		common.PortPostgres,
	)
	if password != "" {
		connStr += fmt.Sprintf(" password=%s", password)
	}
	connCtx, cancel := context.WithTimeout(ctx, common.ConnectivityTimeout)
	defer cancel()
	return pgx.Connect(connCtx, connStr)
}
