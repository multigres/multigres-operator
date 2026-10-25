package status

import (
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestPhaseClearedPredicate(t *testing.T) {
	t.Parallel()

	p := PhaseClearedPredicate(func(c *multigresv1alpha1.Cell) multigresv1alpha1.Phase {
		return c.Status.Phase
	})
	cell := func(phase multigresv1alpha1.Phase) *multigresv1alpha1.Cell {
		c := &multigresv1alpha1.Cell{}
		c.Status.Phase = phase
		return c
	}
	for name, tc := range map[string]struct {
		old, new client.Object
		want     bool
	}{
		"cleared":         {cell(multigresv1alpha1.PhaseHealthy), cell(""), true},
		"still set":       {cell(multigresv1alpha1.PhaseHealthy), cell(multigresv1alpha1.PhaseDegraded), false},
		"set from empty":  {cell(""), cell(multigresv1alpha1.PhaseHealthy), false},
		"empty both":      {cell(""), cell(""), false},
		"other kind":      {&multigresv1alpha1.Shard{}, cell(""), false},
		"other kind, new": {cell(multigresv1alpha1.PhaseHealthy), &multigresv1alpha1.Shard{}, false},
	} {
		if got := p.Update(
			event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new},
		); got != tc.want {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
	if p.Create(event.CreateEvent{Object: cell("")}) ||
		p.Delete(event.DeleteEvent{Object: cell("")}) ||
		p.Generic(event.GenericEvent{Object: cell("")}) {
		t.Error("only updates are admitted")
	}
}
