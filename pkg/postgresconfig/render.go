// Package postgresconfig renders the effective postgresql.conf the operator
// mounts into pgctld. The operator owns config generation end-to-end: a static
// baseline it defines, followed by the user's legacy PostgresConfigRef content
// and the inline spec.postgresConfig map, each appended so it overrides earlier
// layers (PostgreSQL applies later assignments last-write-wins). Resource-
// derived sizing is baked into the Config before rendering.
package postgresconfig

import (
	_ "embed"
	"fmt"
	"sort"
	"strings"
	"text/template"
)

// ConfigFileName is the filename the rendered content is projected to inside
// the pod. pgctld reads it via POSTGRES_INITDB_EXTRA_CONF.
const ConfigFileName = "postgresql.conf"

//go:embed templates/postgresql.conf.tmpl
var baseTemplate string

var parsedBaseTemplate = template.Must(
	template.New("postgresql.conf").Parse(baseTemplate),
)

// Config holds the tunable postgresql.conf values rendered into the baseline
// template. Non-tunable static lines (SSL, locales, logging, wal_level, ...)
// live in the template itself and need no fields here.
type Config struct {
	MaxConnections int

	// Memory settings (postgres size strings, e.g. "128MB").
	SharedBuffers      string
	MaintenanceWorkMem string
	WorkMem            string

	// Worker and parallel settings.
	MaxWorkerProcesses            int
	EffectiveIoConcurrency        int
	MaxParallelWorkers            int
	MaxParallelWorkersPerGather   int
	MaxParallelMaintenanceWorkers int

	// WAL settings.
	WalBuffers         string
	MinWalSize         string
	MaxWalSize         string
	WalKeepSize        string
	MaxSlotWalKeepSize string

	// Checkpoint / replication / planner settings.
	CheckpointCompletionTarget float64
	MaxWalSenders              int
	MaxReplicationSlots        int
	EffectiveCacheSize         string
	RandomPageCost             float64
	DefaultStatisticsTarget    int

	ClusterName string
}

// Defaults returns the operator's built-in static baseline, tuned for a small
// instance. Resource-derived sizing overrides the size-sensitive fields; the
// rest are the shipped baseline.
func Defaults() Config {
	return Config{
		MaxConnections:                60,
		SharedBuffers:                 "64MB",
		MaintenanceWorkMem:            "16MB",
		WorkMem:                       "1092kB",
		MaxWorkerProcesses:            6,
		EffectiveIoConcurrency:        0,
		MaxParallelWorkers:            2,
		MaxParallelWorkersPerGather:   1,
		MaxParallelMaintenanceWorkers: 1,
		WalBuffers:                    "1920kB",
		MinWalSize:                    "80MB",
		MaxWalSize:                    "1024MB",
		WalKeepSize:                   "1000MB",
		MaxSlotWalKeepSize:            "1024MB",
		CheckpointCompletionTarget:    0.9,
		MaxWalSenders:                 25,
		MaxReplicationSlots:           25,
		EffectiveCacheSize:            "192MB",
		RandomPageCost:                1.1,
		DefaultStatisticsTarget:       100,
		ClusterName:                   "default",
	}
}

// Render produces the effective postgresql.conf: the baseline template rendered
// with cfg, followed by the user's legacy PostgresConfigRef content (verbatim)
// and the inline spec.postgresConfig map, each appended so it overrides earlier
// layers. refContent is the body of the user's PostgresConfigRef key, or empty
// when no ref is set; inline may be nil.
func Render(cfg Config, refContent string, inline map[string]string) (string, error) {
	var b strings.Builder
	if err := parsedBaseTemplate.Execute(&b, cfg); err != nil {
		return "", fmt.Errorf("rendering postgres config template: %w", err)
	}

	if trimmed := strings.TrimRight(refContent, "\n"); trimmed != "" {
		b.WriteString("\n# postgresConfigRef\n")
		b.WriteString(trimmed)
		b.WriteString("\n")
	}

	if len(inline) > 0 {
		b.WriteString("\n# spec.postgresConfig\n")
		writeSortedMap(&b, inline)
	}

	return b.String(), nil
}

func writeSortedMap(b *strings.Builder, m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(b, "%s = %s\n", k, quote(m[k]))
	}
}

// quote wraps a GUC value in single quotes, escaping embedded single quotes by
// doubling them. Single-quoted values are accepted in postgresql.conf for every
// GUC type — numbers, booleans, enums, memory units, and comma-separated lists
// — so this is a safe universal representation regardless of the parameter type.
func quote(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}
