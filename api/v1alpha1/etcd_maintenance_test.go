package v1alpha1

import (
	"testing"

	"github.com/multigres/testkit/assert"
)

func TestEffectiveCompaction(t *testing.T) {
	for _, tc := range []struct {
		name            string
		config          *EtcdMaintenanceConfig
		mode, retention string
		wantErr         bool
	}{
		{"omitted", nil, "periodic", "1h", false},
		{"empty", &EtcdMaintenanceConfig{}, "periodic", "1h", false},
		{"periodic", &EtcdMaintenanceConfig{AutoCompactionRetention: "30m"}, "periodic", "30m", false},
		{"revision default", &EtcdMaintenanceConfig{AutoCompactionMode: "revision"}, "revision", "10000", false},
		{"revision", &EtcdMaintenanceConfig{AutoCompactionMode: "revision", AutoCompactionRetention: "5000"}, "revision", "5000", false},
		{"unknown mode", &EtcdMaintenanceConfig{AutoCompactionMode: "bad"}, "", "", true},
		{"disabled retention", &EtcdMaintenanceConfig{AutoCompactionRetention: "0h"}, "", "", true},
		{"negative retention", &EtcdMaintenanceConfig{AutoCompactionRetention: "-1h"}, "", "", true},
		{"invalid duration", &EtcdMaintenanceConfig{AutoCompactionRetention: "forever"}, "", "", true},
		{"revision duration", &EtcdMaintenanceConfig{AutoCompactionMode: "revision", AutoCompactionRetention: "1h"}, "", "", true},
		{"zero revisions", &EtcdMaintenanceConfig{AutoCompactionMode: "revision", AutoCompactionRetention: "0"}, "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode, retention, err := tc.config.EffectiveCompaction()
			assert.NewAborting(t).
				False((err != nil) != tc.wantErr || mode != tc.mode || retention != tc.retention, "got (%q,%q,%v)", mode, retention, err)
		})
	}
}
