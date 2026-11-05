package suite

import (
	"testing"

	"github.com/multigres/multigres-operator/pkg/ownership"
)

func TestMisclaimMatchesOnlyItsOwnViolation(t *testing.T) {
	m := misclaim{
		kind:    "Shard",
		field:   ownership.ConditionPath("StorageClassValid"),
		owner:   ownership.ManagerShardGuard,
		claimer: ownership.ManagerShard,
	}
	const field = `.status.conditions[type="StorageClassValid"].observedGeneration`
	tests := []struct {
		name      string
		violation string
		want      bool
	}{
		// Verbatim from a failing run of
		// TestSelectorImpostorShardOwnerRefReconcileLeavesStrangerPVCs.
		{
			"the misclaim, as reported",
			"Shard pvc-adopt-postgres-default-0-inf-f07875d3: status field .status.conditions[type=\"StorageClassValid\"].observedGeneration owned by multigres-shard-guard, claimed by multigres-shard",
			true,
		},
		{
			"the condition element itself",
			`Shard s-1: status field .status.conditions[type="StorageClassValid"]` +
				" owned by multigres-shard-guard, claimed by multigres-shard",
			true,
		},
		{"another kind", "TopoServer s-1: status field " + field +
			" owned by multigres-shard-guard, claimed by multigres-shard", false},
		{
			"another condition",
			`Shard s-1: status field .status.conditions[type="Available"].status` +
				" owned by multigres-shard-guard, claimed by multigres-shard",
			false,
		},
		{
			"a condition sharing the prefix",
			`Shard s-1: status field .status.conditions[type="StorageClassValidX"].status` +
				" owned by multigres-shard-guard, claimed by multigres-shard",
			false,
		},
		{"another claimer", "Shard s-1: status field " + field +
			" owned by multigres-shard-guard, claimed by suite.test", false},
		{"another owner", "Shard s-1: status field " + field +
			" owned by multigres-shard, claimed by multigres-shard-guard", false},
		{"an unowned field", "Shard s-1: unowned status field " + field +
			" claimed by multigres-shard", false},
		{"a spec overlap", "Shard s-1: field .metadata.labels.x claimed by more than " +
			"one operator manager: multigres-shard, multigres-tablegroup", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := m.matches(tc.violation); got != tc.want {
				t.Errorf("matches(%q) = %v, want %v", tc.violation, got, tc.want)
			}
		})
	}
}

// A field is matched with its descendants, not with every path it prefixes:
// .status.phase does not cover a sibling such as .status.phaseReason.
func TestMisclaimFieldDoesNotCoverSiblingsItPrefixes(t *testing.T) {
	m := misclaim{kind: "Shard", field: ".status.phase", owner: "a", claimer: "b"}
	for violation, want := range map[string]bool{
		"Shard s-1: status field .status.phase owned by a, claimed by b":       true,
		"Shard s-1: status field .status.phase.x owned by a, claimed by b":     true,
		"Shard s-1: status field .status.phaseReason owned by a, claimed by b": false,
	} {
		if got := m.matches(violation); got != want {
			t.Errorf("matches(%q) = %v, want %v", violation, got, want)
		}
	}
}
