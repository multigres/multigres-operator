package report

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// Reporter collects findings from observer checks and emits structured JSON logs.
type Reporter struct {
	mu       sync.Mutex
	findings []Finding
	metrics  *Metrics
	logger   *slog.Logger
}

// NewReporter creates a reporter that emits JSON-structured log lines.
func NewReporter(logger *slog.Logger, metrics *Metrics) *Reporter {
	return &Reporter{
		logger:  logger,
		metrics: metrics,
	}
}

// Report records a finding, emits it as a JSON log line, and updates metrics.
func (r *Reporter) Report(f Finding) {
	if f.Timestamp.IsZero() {
		f.Timestamp = time.Now().UTC()
	}

	r.mu.Lock()
	r.findings = append(r.findings, f)
	r.mu.Unlock()

	if r.metrics != nil {
		r.metrics.RecordFinding(f)
	}

	raw, err := json.Marshal(f)
	if err != nil {
		r.logger.Error("failed to marshal finding", "error", err)
		return
	}

	switch f.Severity {
	case SeverityInfo:
		r.logger.Info(string(raw))
	case SeverityWarn:
		r.logger.Warn(string(raw))
	case SeverityError:
		r.logger.Error(string(raw))
	case SeverityFatal:
		r.logger.Error(string(raw))
	}
}

// SummaryWithFindings returns the findings slice alongside the summary, then resets the buffer.
func (r *Reporter) SummaryWithFindings() ([]Finding, Summary, map[string]bool) {
	return r.collectFindings()
}

func (r *Reporter) collectFindings() ([]Finding, Summary, map[string]bool) {
	r.mu.Lock()
	findings := r.findings
	r.findings = nil
	r.mu.Unlock()

	s := Summary{TotalFindings: len(findings)}
	checkHealthy := make(map[string]bool)

	for _, f := range findings {
		if _, seen := checkHealthy[f.Check]; !seen {
			checkHealthy[f.Check] = true
		}

		switch f.Severity {
		case SeverityInfo:
			s.Counts.Info++
		case SeverityWarn:
			s.Counts.Warn++
		case SeverityError:
			s.Counts.Error++
			checkHealthy[f.Check] = false
		case SeverityFatal:
			s.Counts.Fatal++
			checkHealthy[f.Check] = false
		}
	}
	return findings, s, checkHealthy
}
