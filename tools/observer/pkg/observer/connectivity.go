package observer

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/jackc/pgx/v5"
	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/tools/observer/pkg/common"
	"github.com/multigres/multigres-operator/tools/observer/pkg/report"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func (o *Observer) checkConnectivity(ctx context.Context) {
	o.probeMultiGatewayServices(ctx)
	o.probeMultiOrchServices(ctx)
	o.probeTopoServerServices(ctx)
	o.probePoolPodHealth(ctx)
	o.probeOperatorHealth(ctx)
	o.probeOperatorMetrics(ctx)

	if o.enableSQLProbe {
		o.probeMultiGatewaySQLServices(ctx)
	}

	o.crossCheckReadiness(ctx)
}

func (o *Observer) probeMultiGatewayServices(ctx context.Context) {
	var svcs corev1.ServiceList
	if err := o.client.List(ctx, &svcs,
		o.listOpts(client.MatchingLabels{
			common.LabelAppManagedBy: common.ManagedByMultigres,
			common.LabelAppComponent: common.ComponentMultiGateway,
		})...,
	); err != nil {
		return
	}

	for i := range svcs.Items {
		svc := &svcs.Items[i]
		addr := fmt.Sprintf("%s.%s.svc", svc.Name, svc.Namespace)

		// TCP probe on PG port.
		o.probeTCP(addr, common.PortMultiGatewayPG, "multigateway-pg", svc.Name)

		// HTTP health probes — only on services that expose the HTTP port.
		// The global gateway service only exposes 5432, not 15100.
		if serviceHasPort(svc, common.PortMultiGatewayHTTP) {
			o.probeHTTP(
				ctx,
				addr,
				common.PortMultiGatewayHTTP,
				"/live",
				"multigateway-liveness",
				svc.Name,
			)
			o.probeHTTP(
				ctx,
				addr,
				common.PortMultiGatewayHTTP,
				"/ready",
				"multigateway-readiness",
				svc.Name,
			)
		}
	}
}

func (o *Observer) probeMultiOrchServices(ctx context.Context) {
	var svcs corev1.ServiceList
	if err := o.client.List(ctx, &svcs,
		o.listOpts(client.MatchingLabels{
			common.LabelAppManagedBy: common.ManagedByMultigres,
			common.LabelAppComponent: common.ComponentMultiOrch,
		})...,
	); err != nil {
		return
	}

	for i := range svcs.Items {
		svc := &svcs.Items[i]
		addr := fmt.Sprintf("%s.%s.svc", svc.Name, svc.Namespace)
		o.probeHTTP(ctx, addr, common.PortMultiOrchHTTP, "/live", "multiorch-liveness", svc.Name)
		o.probeHTTP(ctx, addr, common.PortMultiOrchHTTP, "/ready", "multiorch-readiness", svc.Name)
		o.probeMultiOrchStatus(ctx, addr, svc.Name)
	}
}

func (o *Observer) probeTopoServerServices(ctx context.Context) {
	// Probe managed etcd services.
	var svcs corev1.ServiceList
	if err := o.client.List(ctx, &svcs,
		o.listOpts(client.MatchingLabels{
			common.LabelAppManagedBy: common.ManagedByMultigres,
			common.LabelAppComponent: common.ComponentGlobalTopo,
		})...,
	); err != nil {
		return
	}

	for i := range svcs.Items {
		svc := &svcs.Items[i]
		addr := fmt.Sprintf("%s.%s.svc", svc.Name, svc.Namespace)
		o.probeHTTP(ctx, addr, common.PortEtcdClient, "/health", "etcd-health", svc.Name)
	}

	// Probe external etcd endpoints from clusters using external topo servers.
	var clusters multigresv1alpha1.MultigresClusterList
	if err := o.client.List(ctx, &clusters, o.listOpts()...); err != nil {
		return
	}
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		if cluster.Spec.GlobalTopoServer == nil ||
			cluster.Spec.GlobalTopoServer.External == nil {
			continue
		}
		for _, ep := range cluster.Spec.GlobalTopoServer.External.Endpoints {
			parsed, err := url.Parse(string(ep))
			if err != nil {
				continue
			}
			host := parsed.Hostname()
			port := common.PortEtcdClient
			if parsed.Port() != "" {
				if p, err := net.LookupPort("tcp", parsed.Port()); err == nil {
					port = p
				}
			}
			component := fmt.Sprintf("external-etcd/%s/%s", cluster.Name, host)
			o.probeHTTP(ctx, host, port, "/health", "etcd-health", component)
		}
	}
}

func (o *Observer) probePoolPodHealth(ctx context.Context) {
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		o.listOpts(client.MatchingLabels{
			common.LabelAppManagedBy: common.ManagedByMultigres,
			common.LabelAppComponent: common.ComponentPool,
		})...,
	); err != nil {
		return
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.PodIP == "" {
			continue
		}
		if o.isPodInGracePeriod(pod.Name) {
			continue
		}
		o.probeHTTP(
			ctx,
			pod.Status.PodIP,
			common.PortMultiPoolerHTTP,
			"/live",
			"multipooler-health",
			pod.Name,
		)
		o.probeHTTP(
			ctx,
			pod.Status.PodIP,
			common.PortMultiPoolerHTTP,
			"/ready",
			"multipooler-readiness",
			pod.Name,
		)
		o.probePoolPodGRPC(ctx, pod)
	}
}

func (o *Observer) probeMultiOrchStatus(ctx context.Context, host, component string) {
	if o.hasAnyPodInGracePeriod() {
		return
	}

	url := fmt.Sprintf("http://%s:%d/debug/status", host, common.PortMultiOrchHTTP)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}

	start := time.Now()
	resp, err := o.httpClient.Do(req)
	latency := time.Since(start)

	if err != nil {
		if o.probes != nil {
			o.probes.RecordProbe(ProbeResult{
				Check: "multiorch-pooler-health", Component: component, Target: url,
				OK: false, Latency: latency.Round(time.Millisecond).String(), Error: err.Error(),
			})
		}
		return
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		if o.probes != nil {
			o.probes.RecordProbe(ProbeResult{
				Check:     "multiorch-pooler-health",
				Component: component,
				Target:    url,
				OK:        false,
				Latency:   latency.Round(time.Millisecond).String(),
				Error:     fmt.Sprintf("read body: %v", err),
			})
		}
		return
	}

	text := strings.ToLower(string(body))

	errorIndicators := []string{
		"deadlineexceeded",
		"unavailable",
		"unhealthy",
		"connection refused",
	}
	var rawErrors []string
	for _, indicator := range errorIndicators {
		if strings.Contains(text, indicator) {
			rawErrors = append(rawErrors, indicator)
		}
	}

	// Count pooler sections: look for lines referencing pooler endpoints.
	// The /debug/status page lists poolers with their health status.
	lines := strings.Split(string(body), "\n")
	totalPoolers := 0
	unhealthyPoolers := 0
	for _, line := range lines {
		lower := strings.ToLower(line)
		// Pooler entries typically reference the pooler port or "pooler" in the status page.
		if strings.Contains(lower, fmt.Sprintf(":%d", common.PortMultiPoolerGRPC)) ||
			strings.Contains(lower, "multipooler") ||
			strings.Contains(lower, "pooler") {
			totalPoolers++
			for _, indicator := range errorIndicators {
				if strings.Contains(lower, indicator) {
					unhealthyPoolers++
					break
				}
			}
		}
	}

	healthyPoolers := totalPoolers - unhealthyPoolers

	if o.probes != nil {
		errStr := ""
		if len(rawErrors) > 0 {
			errStr = strings.Join(rawErrors, ", ")
		}
		o.probes.RecordProbe(ProbeResult{
			Check:     "multiorch-pooler-health",
			Component: component,
			Target:    url,
			OK: len(
				rawErrors,
			) == 0,
			Latency: latency.Round(time.Millisecond).String(),
			Error:   errStr,
		})
	}

	if totalPoolers > 0 && unhealthyPoolers == totalPoolers {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityFatal,
			Check:     "connectivity",
			Component: component,
			Message: fmt.Sprintf(
				"multiorch-pooler-health: multiorch reports all %d poolers unreachable",
				totalPoolers,
			),
			Details: map[string]any{
				"totalPoolers":     totalPoolers,
				"healthyPoolers":   healthyPoolers,
				"unhealthyPoolers": unhealthyPoolers,
				"rawErrors":        rawErrors,
			},
		})
	} else if unhealthyPoolers > 0 {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "connectivity",
			Component: component,
			Message: fmt.Sprintf(
				"multiorch-pooler-health: %d/%d poolers showing errors",
				unhealthyPoolers,
				totalPoolers,
			),
			Details: map[string]any{
				"totalPoolers":     totalPoolers,
				"healthyPoolers":   healthyPoolers,
				"unhealthyPoolers": unhealthyPoolers,
				"rawErrors":        rawErrors,
			},
		})
	} else if len(rawErrors) > 0 {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "connectivity",
			Component: component,
			Message: fmt.Sprintf(
				"multiorch-pooler-health: /debug/status contains error indicators: %s",
				strings.Join(rawErrors, ", "),
			),
			Details: map[string]any{
				"rawErrors": rawErrors,
			},
		})
	}
}

func (o *Observer) probePoolPodGRPC(ctx context.Context, pod *corev1.Pod) {
	addr := net.JoinHostPort(pod.Status.PodIP, fmt.Sprintf("%d", common.PortMultiPoolerGRPC))

	dialCtx, cancel := context.WithTimeout(ctx, common.GRPCHealthTimeout)
	defer cancel()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		o.recordGRPCProbeResult(pod.Name, addr, false, 0, fmt.Sprintf("dial error: %v", err))
		return
	}
	defer func() { _ = conn.Close() }()

	client := healthpb.NewHealthClient(conn)
	start := time.Now()
	resp, err := client.Check(dialCtx, &healthpb.HealthCheckRequest{})
	latency := time.Since(start)

	if err != nil {
		o.recordGRPCProbeResult(pod.Name, addr, false, latency, err.Error())
		return
	}

	ok := resp.Status == healthpb.HealthCheckResponse_SERVING
	errStr := ""
	if !ok {
		errStr = fmt.Sprintf("status: %s", resp.Status)
	}
	o.recordGRPCProbeResult(pod.Name, addr, ok, latency, errStr)
}

func (o *Observer) recordGRPCProbeResult(
	component, addr string,
	ok bool,
	latency time.Duration,
	errStr string,
) {
	if o.metrics != nil {
		o.metrics.RecordProbeLatency("multipooler-grpc-health", component, latency)
	}

	if o.probes != nil {
		o.probes.RecordProbe(ProbeResult{
			Check: "multipooler-grpc-health", Component: component, Target: addr,
			OK: ok, Latency: latency.Round(time.Millisecond).String(), Error: errStr,
		})
	}

	if !ok {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "connectivity",
			Component: component,
			Message: fmt.Sprintf(
				"multipooler-grpc-health: gRPC health check failed for %s: %s",
				addr,
				errStr,
			),
		})
		return
	}

	if latency > common.ConnectivityLatencyThreshold {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "connectivity",
			Component: component,
			Message: fmt.Sprintf(
				"multipooler-grpc-health: high latency %s for %s",
				latency.Round(time.Millisecond),
				addr,
			),
		})
	}
}

func (o *Observer) probeOperatorHealth(ctx context.Context) {
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		client.InNamespace(o.operatorNamespace),
		client.MatchingLabels{"control-plane": "controller-manager"},
	); err != nil {
		return
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.PodIP == "" {
			continue
		}
		o.probeHTTP(
			ctx,
			pod.Status.PodIP,
			common.PortOperatorHealth,
			"/healthz",
			"operator-health",
			pod.Name,
		)
		o.probeHTTP(
			ctx,
			pod.Status.PodIP,
			common.PortOperatorHealth,
			"/readyz",
			"operator-readiness",
			pod.Name,
		)
	}
}

func (o *Observer) probeTCP(host string, port int, check, component string) {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	start := time.Now()
	conn, err := net.DialTimeout("tcp", addr, common.ConnectivityTimeout)
	latency := time.Since(start)

	if o.metrics != nil {
		o.metrics.RecordProbeLatency(check, component, latency)
	}

	if o.probes != nil {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		o.probes.RecordProbe(ProbeResult{
			Check: check, Component: component, Target: addr,
			OK: err == nil, Latency: latency.Round(time.Millisecond).String(), Error: errStr,
		})
	}

	if err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "connectivity",
			Component: component,
			Message:   fmt.Sprintf("%s: TCP probe failed for %s: %v", check, addr, err),
		})
		return
	}
	_ = conn.Close()

	if latency > common.ConnectivityLatencyThreshold {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "connectivity",
			Component: component,
			Message: fmt.Sprintf(
				"%s: high latency %s connecting to %s",
				check,
				latency.Round(time.Millisecond),
				addr,
			),
		})
	}
}

func (o *Observer) probeHTTP(
	ctx context.Context,
	host string,
	port int,
	path, check, component string,
) {
	url := fmt.Sprintf("http://%s:%d%s", host, port, path)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}

	start := time.Now()
	resp, err := o.httpClient.Do(req)
	latency := time.Since(start)

	if o.metrics != nil {
		o.metrics.RecordProbeLatency(check, component, latency)
	}

	ok := err == nil && resp != nil && resp.StatusCode == http.StatusOK
	if o.probes != nil {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		} else if resp != nil && resp.StatusCode != http.StatusOK {
			errStr = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		o.probes.RecordProbe(ProbeResult{
			Check: check, Component: component, Target: url,
			OK: ok, Latency: latency.Round(time.Millisecond).String(), Error: errStr,
		})
	}

	if err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "connectivity",
			Component: component,
			Message:   fmt.Sprintf("%s: HTTP probe failed for %s: %v", check, url, err),
		})
		return
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		severity := report.SeverityError
		if resp.StatusCode == http.StatusServiceUnavailable {
			severity = report.SeverityWarn
		}
		o.reporter.Report(report.Finding{
			Severity:  severity,
			Check:     "connectivity",
			Component: component,
			Message:   fmt.Sprintf("%s: %s returned HTTP %d", check, url, resp.StatusCode),
			Details:   map[string]any{"statusCode": resp.StatusCode},
		})
		return
	}

	if latency > common.ConnectivityLatencyThreshold {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "connectivity",
			Component: component,
			Message: fmt.Sprintf(
				"%s: high latency %s for %s",
				check,
				latency.Round(time.Millisecond),
				url,
			),
		})
	}
}

func (o *Observer) crossCheckReadiness(ctx context.Context) {
	if o.probes == nil {
		return
	}

	probeResults, _ := o.probes.Data()["connectivity"].([]ProbeResult)
	if len(probeResults) == 0 {
		return
	}

	// Index probe results by component+check for pool pods (probed by pod name),
	// and by check name only for service-level probes (multiorch, multigateway).
	type probeKey struct {
		component string
		check     string
	}
	probeByKey := make(map[probeKey]ProbeResult, len(probeResults))
	// Collect all failing service-level probes by check name.
	failedByCheck := make(map[string][]ProbeResult)
	for _, pr := range probeResults {
		probeByKey[probeKey{pr.Component, pr.Check}] = pr
		if !pr.OK {
			failedByCheck[pr.Check] = append(failedByCheck[pr.Check], pr)
		}
	}

	// Get pod data from the pods check.
	podDataRaw, _ := o.probes.Data()["pods"].(map[string]any)
	if podDataRaw == nil {
		return
	}
	podList, _ := podDataRaw["pods"].([]map[string]any)

	for _, pd := range podList {
		name, _ := pd["name"].(string)
		ready, _ := pd["ready"].(bool)
		component, _ := pd["component"].(string)

		if !ready {
			continue
		}
		if o.isPodInGracePeriod(name) {
			continue
		}

		switch component {
		case common.ComponentPool:
			// Pool pod probes are keyed by pod name.
			grpcProbe, hasGRPC := probeByKey[probeKey{name, "multipooler-grpc-health"}]
			readinessProbe, hasReadiness := probeByKey[probeKey{name, "multipooler-readiness"}]

			if hasGRPC && !grpcProbe.OK {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityFatal,
					Check:     "connectivity",
					Component: name,
					Message: fmt.Sprintf(
						"Pod %s reports Ready but multipooler-grpc-health probe failed: %s — Kubernetes readiness probe is not detecting the failure",
						name,
						grpcProbe.Error,
					),
					Details: map[string]any{
						"kubernetesReady": true,
						"grpcProbeOK":     false,
						"grpcProbeError":  grpcProbe.Error,
					},
				})
			}

			if hasReadiness && !readinessProbe.OK {
				livenessProbe, hasLiveness := probeByKey[probeKey{name, "multipooler-health"}]
				if hasLiveness && livenessProbe.OK {
					o.reporter.Report(report.Finding{
						Severity:  report.SeverityError,
						Check:     "connectivity",
						Component: name,
						Message: fmt.Sprintf(
							"Pod %s reports Ready but multipooler-readiness returned %s — liveness passes but readiness fails",
							name,
							readinessProbe.Error,
						),
						Details: map[string]any{
							"kubernetesReady":  true,
							"readinessProbeOK": false,
							"readinessError":   readinessProbe.Error,
							"livenessProbeOK":  true,
						},
					})
				}
			}

		case common.ComponentMultiOrch:
			// Multiorch probes are service-level. Check if any service-level probe failed.
			if failed := failedByCheck["multiorch-readiness"]; len(failed) > 0 {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityFatal,
					Check:     "connectivity",
					Component: name,
					Message: fmt.Sprintf(
						"Pod %s reports Ready but multiorch-readiness probe failed — Kubernetes readiness probe is not detecting the failure",
						name,
					),
					Details: map[string]any{
						"kubernetesReady":  true,
						"readinessProbeOK": false,
						"failedServices":   probeComponents(failed),
					},
				})
			}

			if failed := failedByCheck["multiorch-pooler-health"]; len(failed) > 0 {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityFatal,
					Check:     "connectivity",
					Component: name,
					Message: fmt.Sprintf(
						"Pod %s reports Ready but multiorch-pooler-health found errors — orchestrator is healthy but its poolers are not",
						name,
					),
					Details: map[string]any{
						"kubernetesReady":     true,
						"poolerHealthProbeOK": false,
						"failedServices":      probeComponents(failed),
					},
				})
			}

		case common.ComponentMultiGateway:
			if failed := failedByCheck["multigateway-readiness"]; len(failed) > 0 {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityError,
					Check:     "connectivity",
					Component: name,
					Message: fmt.Sprintf(
						"Pod %s reports Ready but multigateway-readiness probe failed — Kubernetes readiness probe is not detecting the failure",
						name,
					),
					Details: map[string]any{
						"kubernetesReady":  true,
						"readinessProbeOK": false,
						"failedServices":   probeComponents(failed),
					},
				})
			}
		}
	}
}

func probeComponents(probes []ProbeResult) []string {
	out := make([]string, len(probes))
	for i, p := range probes {
		out[i] = p.Component
	}
	return out
}

func (o *Observer) probeMultiGatewaySQLServices(ctx context.Context) {
	if o.hasAnyPodInGracePeriod() {
		return
	}

	var svcs corev1.ServiceList
	if err := o.client.List(ctx, &svcs,
		o.listOpts(client.MatchingLabels{
			common.LabelAppManagedBy: common.ManagedByMultigres,
			common.LabelAppComponent: common.ComponentMultiGateway,
		})...,
	); err != nil {
		return
	}

	for i := range svcs.Items {
		svc := &svcs.Items[i]
		addr := fmt.Sprintf("%s.%s.svc", svc.Name, svc.Namespace)
		password := o.fetchGatewayPassword(ctx, svc)
		o.probeSQL(ctx, addr, common.PortMultiGatewayPG, svc.Name, password)
	}
}

// probeSQL connects to a PostgreSQL-compatible endpoint and runs SELECT 1.
//
// Uses simple query protocol because the multigateway does not yet support the
// extended query protocol's Describe step (fails with SQLSTATE MTD06). pgx defaults
// to extended protocol (Parse → Describe → Bind → Execute), which breaks through
// the gateway. Simple protocol sends the query as a single 'Q' message, which the
// gateway handles correctly.
func (o *Observer) probeSQL(
	ctx context.Context,
	host string,
	port int,
	component, password string,
) {
	if password == "" {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "connectivity",
			Component: component,
			Message: fmt.Sprintf(
				"sql-probe skipped for %s:%d: could not fetch postgres password from shard secret",
				host,
				port,
			),
		})
		return
	}

	connStr := fmt.Sprintf(
		"host=%s port=%d user=postgres dbname=postgres connect_timeout=5 sslmode=disable password=%s",
		host,
		port,
		password,
	)

	probeCtx, cancel := context.WithTimeout(ctx, common.ConnectivityTimeout)
	defer cancel()

	connCfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "connectivity",
			Component: component,
			Message: fmt.Sprintf(
				"sql-probe: invalid connection config for %s:%d: %v",
				host,
				port,
				err,
			),
		})
		return
	}
	connCfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	start := time.Now()
	conn, err := pgx.ConnectConfig(probeCtx, connCfg)
	if err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "connectivity",
			Component: component,
			Message:   fmt.Sprintf("sql-probe: failed to connect to %s:%d: %v", host, port, err),
		})
		return
	}
	defer func() { _ = conn.Close(probeCtx) }()

	var result int
	if err := conn.QueryRow(probeCtx, "SELECT 1").Scan(&result); err != nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "connectivity",
			Component: component,
			Message:   fmt.Sprintf("sql-probe: SELECT 1 failed on %s:%d: %v", host, port, err),
		})
		return
	}

	latency := time.Since(start)
	if o.metrics != nil {
		o.metrics.RecordProbeLatency("sql-probe", component, latency)
	}

	if o.probes != nil {
		o.probes.RecordProbe(ProbeResult{
			Check: "sql-probe", Component: component,
			Target:  fmt.Sprintf("%s:%d", host, port),
			OK:      true,
			Latency: latency.Round(time.Millisecond).String(),
		})
	}

	if latency > common.ConnectivityLatencyThreshold {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "connectivity",
			Component: component,
			Message: fmt.Sprintf(
				"sql-probe: high latency %s for %s:%d",
				latency.Round(time.Millisecond),
				host,
				port,
			),
		})
	}
}

// fetchGatewayPassword looks up a postgres password for the gateway's cluster.
// It finds a shard belonging to the same cluster and reads its password secret.
// Tries cluster label first, then falls back to instance label (per-zone services
// carry app.kubernetes.io/instance but not multigres.com/cluster).
func (o *Observer) fetchGatewayPassword(ctx context.Context, svc *corev1.Service) string {
	// Try cluster label first (global gateway services).
	if clusterName := svc.Labels[common.LabelMultigresCluster]; clusterName != "" {
		var shards multigresv1alpha1.ShardList
		if err := o.client.List(ctx, &shards,
			client.InNamespace(svc.Namespace),
			client.MatchingLabels{common.LabelMultigresCluster: clusterName},
		); err == nil && len(shards.Items) > 0 {
			return o.fetchShardPassword(ctx, &shards.Items[0])
		}
	}

	// Fall back to instance label (per-zone gateway services).
	if instance := svc.Labels[common.LabelAppInstance]; instance != "" {
		var shards multigresv1alpha1.ShardList
		if err := o.client.List(ctx, &shards,
			client.InNamespace(svc.Namespace),
			client.MatchingLabels{common.LabelMultigresCluster: instance},
		); err == nil && len(shards.Items) > 0 {
			return o.fetchShardPassword(ctx, &shards.Items[0])
		}
	}

	return ""
}

// fetchShardPassword reads the postgres superuser password from the Secret referenced by the Shard.
func (o *Observer) fetchShardPassword(ctx context.Context, shard *multigresv1alpha1.Shard) string {
	secretName, secretKey, ok := o.fetchShardPasswordSecretRef(ctx, shard)
	if !ok {
		o.logger.Debug(
			"shard is missing postgres password secret reference",
			"shard",
			shard.Name,
			"namespace",
			shard.Namespace,
		)
		return ""
	}

	var secret corev1.Secret
	if err := o.client.Get(ctx, types.NamespacedName{
		Namespace: shard.Namespace,
		Name:      secretName,
	}, &secret); err != nil {
		o.logger.Debug(
			"failed to read postgres password secret",
			"secret",
			secretName,
			"error",
			err,
		)
		return ""
	}

	pw, ok := secret.Data[secretKey]
	if !ok {
		o.logger.Debug(
			"postgres password secret is missing key",
			"secret",
			secretName,
			"key",
			secretKey,
		)
		return ""
	}
	return string(pw)
}

func (o *Observer) fetchShardPasswordSecretRef(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (name, key string, ok bool) {
	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "multigres.com",
		Version: "v1alpha1",
		Kind:    "Shard",
	})
	if err := o.client.Get(ctx, types.NamespacedName{
		Namespace: shard.Namespace,
		Name:      shard.Name,
	}, current); err != nil {
		o.logger.Debug(
			"failed to read shard for postgres password secret reference",
			"shard",
			shard.Name,
			"namespace",
			shard.Namespace,
			"error",
			err,
		)
		return "", "", false
	}

	name, nameFound, _ := unstructured.NestedString(
		current.Object,
		"spec",
		"postgresPasswordSecretRef",
		"name",
	)
	key, keyFound, _ := unstructured.NestedString(
		current.Object,
		"spec",
		"postgresPasswordSecretRef",
		"key",
	)
	return name, key, nameFound && keyFound && name != "" && key != ""
}

// probeOperatorMetrics checks that the operator's /metrics endpoint is reachable
// and contains expected metric names. The operator serves metrics over HTTPS on
// port 8443, so we use a TLS-skipping client for cluster-internal probing.
func (o *Observer) probeOperatorMetrics(ctx context.Context) {
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		client.InNamespace(o.operatorNamespace),
		client.MatchingLabels{"control-plane": "controller-manager"},
	); err != nil {
		return
	}

	tlsClient := &http.Client{
		Timeout:   common.MetricsProbeTimeout,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec
	}

	expectedMetrics := []string{
		"multigres_operator_cluster_info",
		"multigres_operator_webhook_request_total",
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning {
			continue
		}

		url := fmt.Sprintf("https://%s:%d/metrics", pod.Status.PodIP, common.PortOperatorMetrics)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}

		resp, err := tlsClient.Do(req)
		if err != nil {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityWarn,
				Check:     "connectivity",
				Component: pod.Name,
				Message: fmt.Sprintf(
					"operator-metrics: /metrics endpoint unreachable on %s: %v",
					pod.Name, err,
				),
			})
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			// Authenticated metrics endpoint — reachable but requires RBAC.
			continue
		}
		if resp.StatusCode != http.StatusOK {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityWarn,
				Check:     "connectivity",
				Component: pod.Name,
				Message: fmt.Sprintf(
					"operator-metrics: /metrics returned HTTP %d on %s",
					resp.StatusCode, pod.Name,
				),
			})
			continue
		}

		bodyStr := string(body)
		for _, metric := range expectedMetrics {
			if !strings.Contains(bodyStr, metric) {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityWarn,
					Check:     "connectivity",
					Component: pod.Name,
					Message: fmt.Sprintf(
						"operator-metrics: expected metric %q not found in /metrics output on %s",
						metric, pod.Name,
					),
				})
			}
		}
	}
}

// serviceHasPort returns true if the service declares the given port.
func serviceHasPort(svc *corev1.Service, port int) bool {
	for _, p := range svc.Spec.Ports {
		if int(p.Port) == port {
			return true
		}
	}
	return false
}
