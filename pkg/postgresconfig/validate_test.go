package postgresconfig

import (
	"strings"
	"testing"
)

func TestValidate_Accepts(t *testing.T) {
	tests := map[string]map[string]string{
		"empty is fine":            nil,
		"known integer, plain":     {"max_connections": "200"},
		"known integer with unit":  {"shared_buffers": "128MB"},
		"known integer, time unit": {"statement_timeout": "5min"},
		"known bool forms": {
			"fsync":         "on",
			"wal_log_hints": "true",
			"hot_standby":   "0",
		},
		"known real":                     {"random_page_cost": "1.1"},
		"known enum (value not checked)": {"log_statement": "ddl"},
		"known string":                   {"log_line_prefix": "%m [%p] "},
		"case-insensitive name":          {"Max_Connections": "200"},
		"namespaced extension param": {
			"cron.database_name":            "postgres",
			"auto_explain.log_min_duration": "10s",
		},
	}
	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			if err := Validate(cfg); err != nil {
				t.Errorf("Validate(%v) = %v, want nil", cfg, err)
			}
		})
	}
}

func TestValidate_Rejects(t *testing.T) {
	tests := map[string]struct {
		cfg      map[string]string
		wantSubs []string
	}{
		"unknown name": {
			cfg:      map[string]string{"maxx_connections": "200"},
			wantSubs: []string{"unknown parameter", "maxx_connections"},
		},
		"bool value for integer": {
			cfg:      map[string]string{"max_connections": "yes"},
			wantSubs: []string{"max_connections", "integer"},
		},
		"non-bool for bool": {
			cfg:      map[string]string{"fsync": "maybe"},
			wantSubs: []string{"fsync", "bool"},
		},
		"non-numeric for real": {
			cfg:      map[string]string{"random_page_cost": "cheap"},
			wantSubs: []string{"random_page_cost", "real"},
		},
		"fractional for integer": {
			cfg:      map[string]string{"max_connections": "1.5"},
			wantSubs: []string{"max_connections", "integer"},
		},
		"operator-managed wal_level": {
			cfg:      map[string]string{"wal_level": "replica"},
			wantSubs: []string{"wal_level", "managed by the operator"},
		},
		"operator-managed connection param": {
			cfg:      map[string]string{"listen_addresses": "127.0.0.1"},
			wantSubs: []string{"listen_addresses", "managed by the operator"},
		},
		"operator-managed, case-insensitive": {
			cfg:      map[string]string{"WAL_LEVEL": "minimal"},
			wantSubs: []string{"managed by the operator"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := Validate(tc.cfg)
			if err == nil {
				t.Fatalf("Validate(%v) = nil, want error", tc.cfg)
			}
			for _, sub := range tc.wantSubs {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("error %q missing %q", err.Error(), sub)
				}
			}
		})
	}
}

func TestValidate_AggregatesAllProblems(t *testing.T) {
	err := Validate(map[string]string{
		"maxx_connections": "200",   // unknown
		"fsync":            "maybe", // bad bool
		"max_connections":  "200",   // valid — should not appear
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "maxx_connections") ||
		!strings.Contains(err.Error(), "fsync") {
		t.Errorf("error should mention both problems: %v", err)
	}
	if strings.Contains(err.Error(), `"max_connections"`) {
		t.Errorf("error should not flag the valid parameter: %v", err)
	}
}

func TestCatalogLoaded(t *testing.T) {
	// The embedded catalog must be non-trivial and contain well-known params.
	if len(catalog) < 300 {
		t.Errorf("catalog has %d entries, expected the full PG17 set (~382)", len(catalog))
	}
	for name, want := range map[string]gucType{
		"max_connections":  gucInteger,
		"shared_buffers":   gucInteger,
		"fsync":            gucBool,
		"random_page_cost": gucReal,
		"wal_level":        gucEnum,
		"log_line_prefix":  gucString,
	} {
		if got := catalog[name]; got != want {
			t.Errorf("catalog[%q] = %q, want %q", name, got, want)
		}
	}
}
