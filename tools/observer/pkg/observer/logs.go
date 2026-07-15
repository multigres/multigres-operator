package observer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/tools/observer/pkg/common"
	"github.com/multigres/multigres-operator/tools/observer/pkg/report"
)

// errorPattern defines a log pattern to watch for with its severity.
type errorPattern struct {
	substring string
	severity  report.Severity
	check     string
}

var operatorErrorPatterns = []errorPattern{
	{"error reconciling", report.SeverityError, "operator-logs"},
	{"failed to", report.SeverityError, "operator-logs"},
	{"stuck in Terminating", report.SeverityError, "operator-logs"},
	{"topology error", report.SeverityError, "operator-logs"},
	{"status error", report.SeverityError, "operator-logs"},
	{"panic:", report.SeverityFatal, "operator-logs"},
	{"runtime error:", report.SeverityFatal, "operator-logs"},
	{"backup stale", report.SeverityWarn, "operator-logs"},
	{"pod replaced", report.SeverityWarn, "operator-logs"},
	{"config error", report.SeverityWarn, "operator-logs"},
	{"expand PVC failed", report.SeverityWarn, "operator-logs"},
}

var dataPlaneErrorPatterns = []errorPattern{
	{"connection refused", report.SeverityError, "dataplane-logs"},
	{"connection reset", report.SeverityError, "dataplane-logs"},
	{"topology registration", report.SeverityError, "dataplane-logs"},
	{"replication error", report.SeverityError, "dataplane-logs"},
	{"FATAL", report.SeverityError, "dataplane-logs"},
	{"panic:", report.SeverityFatal, "dataplane-logs"},
	{"OOM", report.SeverityError, "dataplane-logs"},
	{"out of memory", report.SeverityError, "dataplane-logs"},
}

func (o *Observer) checkLogs(ctx context.Context) {
	sinceSeconds := int64(o.interval.Seconds()) + 5
	if o.lastLogCheck.IsZero() {
		sinceSeconds = 30
	}
	o.lastLogCheck = time.Now()

	// Check operator logs.
	o.checkOperatorLogs(ctx, sinceSeconds)

	// Check data plane component logs (pool pods, multigateway, multiorch, etcd).
	o.checkDataPlaneLogs(ctx, sinceSeconds)
}

func (o *Observer) checkOperatorLogs(ctx context.Context, sinceSeconds int64) {
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		client.InNamespace(o.operatorNamespace),
		client.MatchingLabels{"control-plane": "controller-manager"},
	); err != nil {
		return
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		o.scanPodLogs(ctx, pod, "manager", sinceSeconds, operatorErrorPatterns, false)
	}
}

func (o *Observer) checkDataPlaneLogs(ctx context.Context, sinceSeconds int64) {
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		o.listOpts(client.MatchingLabels{common.LabelAppManagedBy: common.ManagedByMultigres})...,
	); err != nil {
		return
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		comp := pod.Labels[common.LabelAppComponent]
		switch comp {
		case common.ComponentPool:
			if o.isPodInGracePeriod(pod.Name) {
				continue
			}
			o.scanPodLogs(ctx, pod, "multipooler", sinceSeconds, dataPlaneErrorPatterns, false)
			o.scanPodLogs(ctx, pod, "postgres", sinceSeconds, dataPlaneErrorPatterns, false)
		case common.ComponentMultigateway:
			if o.hasAnyPodInGracePeriod() {
				continue
			}
			o.scanPodLogs(ctx, pod, "", sinceSeconds, dataPlaneErrorPatterns, true)
		case common.ComponentMultiorch:
			if o.hasAnyPodInGracePeriod() {
				continue
			}
			o.scanPodLogs(ctx, pod, "", sinceSeconds, dataPlaneErrorPatterns, false)
		case common.ComponentGlobalTopo:
			o.scanPodLogs(ctx, pod, "", sinceSeconds, dataPlaneErrorPatterns, false)
		}
	}
}

type logMatch struct {
	count     int
	firstLine string
	severity  report.Severity
	check     string
}

// isProbeNoise returns true if a JSON log entry is caused by TCP health check
// probes (observer, Kubernetes, load balancers) connecting to a PostgreSQL-
// compatible port without completing the startup handshake.
func isProbeNoise(msg, errMsg string) bool {
	if !strings.HasSuffix(errMsg, "EOF") {
		return false
	}
	switch msg {
	case "startup failed", "error handling message":
		return true
	}
	return false
}

func (o *Observer) scanPodLogs(
	ctx context.Context,
	pod *corev1.Pod,
	container string,
	sinceSeconds int64,
	patterns []errorPattern,
	filterProbeNoise bool,
) {
	opts := &corev1.PodLogOptions{
		SinceSeconds: &sinceSeconds,
	}
	if container != "" {
		opts.Container = container
	}

	req := o.clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		// Don't report log access errors for pods that may be initializing.
		if pod.Status.Phase == corev1.PodRunning {
			o.logger.Debug(
				"failed to stream logs",
				"pod",
				pod.Name,
				"container",
				container,
				"error",
				err,
			)
		}
		return
	}
	defer func() { _ = stream.Close() }()

	component := componentForPod(pod)
	if container != "" {
		component += "/" + container
	}

	scanner := bufio.NewScanner(stream)
	// Limit individual line size to prevent memory issues.
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)

	lineCount := 0
	maxLines := o.logTailLines
	if maxLines <= 0 {
		maxLines = 100
	}

	matches := make(map[string]*logMatch)

	for scanner.Scan() {
		lineCount++
		if lineCount > maxLines {
			break
		}

		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)

		// 1. Try to parse as JSON log
		if strings.HasPrefix(trimmedLine, "{") {
			var jLog struct {
				Level       string `json:"level"`
				Message     string `json:"msg"`
				Error       string `json:"error"`
				ProblemCode string `json:"problem_code"`
			}
			if err := json.Unmarshal([]byte(trimmedLine), &jLog); err == nil {
				lvl := strings.ToLower(jLog.Level)
				if lvl == "error" || lvl == "fatal" {
					if filterProbeNoise && isProbeNoise(jLog.Message, jLog.Error) {
						continue
					}
					sev := report.SeverityError
					if lvl == "fatal" {
						sev = report.SeverityFatal
					}

					// Elevate known critical cluster-breaking errors that hide behind JSON ERROR levels
					if strings.Contains(line, "ShardNeedsBootstrap") || strings.Contains(strings.ToLower(line), "quorum") {
						sev = report.SeverityFatal
					}

					key := jLog.Message
					if jLog.ProblemCode != "" {
						key += ": " + jLog.ProblemCode
					}
					if key == "" {
						key = jLog.Error
					}
					if key == "" {
						key = "generic json error"
					}

					checkName := "dataplane-logs"
					if len(patterns) > 0 {
						checkName = patterns[0].check
					}

					if m, ok := matches[key]; ok {
						m.count++
						if sev == report.SeverityFatal && m.severity != report.SeverityFatal {
							m.severity = report.SeverityFatal
							m.firstLine = truncate(line, 500)
						}
					} else {
						matches[key] = &logMatch{
							count:     1,
							firstLine: truncate(line, 500),
							severity:  sev,
							check:     checkName,
						}
					}
				}
				continue
			}
		}

		// 2. Fallback to raw substring matching for non-JSON lines
		for _, p := range patterns {
			if strings.Contains(strings.ToLower(line), strings.ToLower(p.substring)) {
				// Skip benign Go runtime panics from graceful shutdowns.
				if p.substring == "panic:" && isShutdownPanic(line) {
					break
				}
				if m, ok := matches[p.substring]; ok {
					m.count++
				} else {
					matches[p.substring] = &logMatch{
						count:     1,
						firstLine: truncate(line, 500),
						severity:  p.severity,
						check:     p.check,
					}
				}
				break
			}
		}
	}

	for pattern, m := range matches {
		msg := fmt.Sprintf(
			"Pattern %q matched %d times in %s/%s",
			pattern,
			m.count,
			pod.Name,
			container,
		)
		if m.count == 1 {
			msg = fmt.Sprintf("Pattern %q matched 1 time in %s/%s", pattern, pod.Name, container)
		}
		o.reporter.Report(report.Finding{
			Severity:  o.effectiveSeverity(pod.Name, m.severity),
			Check:     m.check,
			Component: component,
			Message:   msg,
			Details: map[string]any{
				"pod":        pod.Name,
				"container":  container,
				"pattern":    pattern,
				"matchCount": m.count,
				"sampleLine": m.firstLine,
			},
		})
	}
}

// shutdownPanicPatterns are strings that co-occur with "panic:" in log lines
// produced by Go's runtime during graceful process shutdown (e.g. pgctld
// receiving SIGTERM during a rolling restart). These are benign.
var shutdownPanicPatterns = []string{
	"signal: terminated",
	"context canceled",
	"context deadline exceeded",
	"use of closed network connection",
}

// isShutdownPanic returns true if a log line containing "panic:" also contains
// a pattern indicating a benign Go runtime shutdown rather than a real crash.
func isShutdownPanic(line string) bool {
	lower := strings.ToLower(line)
	for _, p := range shutdownPanicPatterns {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
