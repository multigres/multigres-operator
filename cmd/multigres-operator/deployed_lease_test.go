package main

import (
	"strings"
	"testing"
)

func TestDeployedOperatorConflict(t *testing.T) {
	tests := map[string]struct {
		namespaces    []string
		wantErrSubstr string
	}{
		"no lease, as on a bare Kind cluster, allows the run": {},
		"a lease in the default install namespace refuses": {
			namespaces:    []string{"multigres-operator"},
			wantErrSubstr: "namespace(s) multigres-operator,",
		},
		"a lease in any other namespace refuses, naming it": {
			namespaces:    []string{"mgo"},
			wantErrSubstr: "namespace(s) mgo,",
		},
		"several leases are all named": {
			namespaces:    []string{"a", "b"},
			wantErrSubstr: "namespace(s) a, b,",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := deployedOperatorConflict(tc.namespaces)
			if tc.wantErrSubstr == "" {
				if err != nil {
					t.Fatalf("deployedOperatorConflict() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf(
					"deployedOperatorConflict() = nil, want an error mentioning %q",
					tc.wantErrSubstr,
				)
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf(
					"deployedOperatorConflict() = %q, want it to mention %q",
					err,
					tc.wantErrSubstr,
				)
			}
		})
	}
}
