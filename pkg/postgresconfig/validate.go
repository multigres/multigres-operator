package postgresconfig

import (
	"bufio"
	_ "embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// pgSettingsCatalog maps each built-in PostgreSQL parameter to its broad type.
// It is generated from PostgreSQL's src/backend/utils/misc/guc_tables.c
// (REL_17_5, matching the operator's default Postgres image). Regenerate for a
// new major version by re-extracting name+type from that file.
//
//go:embed catalog/pg_settings_17.txt
var pgSettingsCatalog string

// gucType is the broad PostgreSQL parameter type used for rough value checks.
type gucType string

const (
	gucBool    gucType = "bool"
	gucInteger gucType = "integer"
	gucReal    gucType = "real"
	gucString  gucType = "string"
	gucEnum    gucType = "enum"
)

var catalog = parseCatalog(pgSettingsCatalog)

// managedGUCs are parameters the operator owns; a user override in
// spec.postgresConfig is rejected rather than silently mis-served. Two reasons:
//
//   - pgctld pins the connection/path parameters on the postgres command line,
//     which wins over any postgresql.conf line, so a user value would be
//     silently ignored — a confusing no-op rather than an honest rejection.
//   - wal_level must stay "logical": multigres' replication and CDC depend on
//     it, and lowering it (once the mounted config is re-read on restart) breaks
//     the control plane.
//
// The value is the reason surfaced in the admission error.
var managedGUCs = map[string]string{
	"port":                    "pinned by the operator on the pgctld command line",
	"listen_addresses":        "pinned by the operator on the pgctld command line",
	"unix_socket_directories": "pinned by the operator on the pgctld command line",
	"data_directory":          "pinned by the operator on the pgctld command line",
	"hba_file":                "pinned by the operator on the pgctld command line",
	"ident_file":              "pinned by the operator on the pgctld command line",
	"wal_level":               "managed by the operator; multigres requires wal_level=logical",
}

func parseCatalog(data string) map[string]gucType {
	m := make(map[string]gucType)
	sc := bufio.NewScanner(strings.NewReader(data))
	for sc.Scan() {
		name, typ, ok := strings.Cut(strings.TrimSpace(sc.Text()), "\t")
		if !ok {
			continue
		}
		m[name] = gucType(typ)
	}
	return m
}

// Validate checks each parameter in a spec.postgresConfig map: the name must be
// a known PostgreSQL parameter (or a namespaced extension parameter such as
// "cron.database_name"), and the value must roughly match the parameter's type.
// It returns a single error listing every problem it finds, or nil.
//
// Validation is deliberately rough: it catches unknown names and gross type
// mismatches (e.g. a bool value for an integer parameter), not every invalid
// value. PostgreSQL performs authoritative validation when the server starts.
func Validate(cfg map[string]string) error {
	if len(cfg) == 0 {
		return nil
	}

	names := make([]string, 0, len(cfg))
	for k := range cfg {
		names = append(names, k)
	}
	sort.Strings(names) // deterministic error messages

	var problems []string
	for _, name := range names {
		if err := validateGUC(name, cfg[name]); err != nil {
			problems = append(problems, err.Error())
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid postgresConfig: %s", strings.Join(problems, "; "))
	}
	return nil
}

func validateGUC(name, value string) error {
	// Namespaced (extension) parameters like "cron.database_name" are custom
	// placeholders PostgreSQL accepts even without the extension loaded, so their
	// names and values cannot be checked against the built-in catalog.
	if strings.Contains(name, ".") {
		return nil
	}
	// PostgreSQL parameter names are case-insensitive; the catalog is lower-case.
	lower := strings.ToLower(strings.TrimSpace(name))
	if reason, managed := managedGUCs[lower]; managed {
		return fmt.Errorf(
			"parameter %q is managed by the operator and cannot be set: %s",
			name,
			reason,
		)
	}
	typ, ok := catalog[lower]
	if !ok {
		return fmt.Errorf("unknown parameter %q", name)
	}
	if !valueMatchesType(value, typ) {
		return fmt.Errorf("parameter %q expects a %s value, got %q", name, typ, value)
	}
	return nil
}

func valueMatchesType(value string, typ gucType) bool {
	v := strings.TrimSpace(value)
	switch typ {
	case gucBool:
		switch strings.ToLower(v) {
		case "on", "off", "true", "false", "yes", "no", "1", "0":
			return true
		}
		return false
	case gucInteger:
		return isIntegerWithOptionalUnit(v)
	case gucReal:
		_, err := strconv.ParseFloat(v, 64)
		return err == nil
	case gucEnum, gucString:
		// Rough: the name is verified against the catalog; specific enum/string
		// values are left to PostgreSQL to validate at startup.
		return true
	}
	return true
}

// isIntegerWithOptionalUnit accepts an integer optionally followed by a memory
// or time unit (e.g. "200", "128MB", "5min"). It does not verify the unit is
// valid for the specific parameter — PostgreSQL does that at startup.
func isIntegerWithOptionalUnit(v string) bool {
	v = strings.TrimSpace(v)
	i := 0
	if i < len(v) && (v[i] == '-' || v[i] == '+') {
		i++
	}
	digits := 0
	for i < len(v) && v[i] >= '0' && v[i] <= '9' {
		i++
		digits++
	}
	if digits == 0 {
		return false
	}
	unit := strings.TrimSpace(v[i:])
	for _, r := range unit {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}
