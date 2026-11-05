package suite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// TestCellsAppendOnly asserts that a settled cluster's spec.cells cannot be
// taken away by any of the three writes a user can make that drop it, and that
// its Cell is left alone afterwards: a JSON patch removing the field, a JSON
// patch emptying the list, and an update from a manifest that omits it (which
// is what a typed Go client sends for both nil and an empty slice, since the
// field is omitempty). Appending a cell must still be accepted.
//
// Each dropped write would make every Cell an orphan for reconcileCells, which
// stamps PendingDeletion and deletes the Cell once it drains. The suite runs no
// admission webhooks, so the CRD's CEL rules are the only thing standing
// between these writes and that drain, which is exactly what this pins.
func TestCellsAppendOnly(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("scenario-cells-append")
	c.WaitForClusterHealthy(cluster)
	clusterKey := client.ObjectKeyFromObject(cluster)

	cell := c.onlyCell(cluster)
	cellKey, cellUID := client.ObjectKeyFromObject(cell), cell.UID

	drops := []struct {
		name  string
		write func(got *MultigresCluster) error
	}{
		{"JSON patch removing spec.cells", func(got *MultigresCluster) error {
			return c.Patch(got, client.RawPatch(types.JSONPatchType,
				[]byte(`[{"op":"remove","path":"/spec/cells"}]`)))
		}},
		{"JSON patch emptying spec.cells", func(got *MultigresCluster) error {
			return c.Patch(got, client.RawPatch(types.JSONPatchType,
				[]byte(`[{"op":"replace","path":"/spec/cells","value":[]}]`)))
		}},
		{"update from a manifest without spec.cells", func(got *MultigresCluster) error {
			got.Spec.Cells = nil
			return c.Update(got)
		}},
	}
	for _, drop := range drops {
		got := &MultigresCluster{}
		c.NoError(c.Get(clusterKey, got), "get cluster")
		err := drop.write(got)
		c.Check().True(apierrors.IsInvalid(err) && strings.Contains(err.Error(), "Append-Only"),
			"%s: want an Invalid append-only rejection, got %v", drop.name, err)
		if err == nil {
			// Put the cell back so the next write starts from the same spec
			// and reports on its own.
			c.updateCluster(cluster, func(got *MultigresCluster) {
				got.Spec.Cells = []CellConfig{{Name: defaultSimCell, ZoneID: "us-central1-a"}}
			})
		}
	}

	stored := &MultigresCluster{}
	c.NoError(c.Get(clusterKey, stored), "get cluster")
	c.Len(stored.Spec.Cells, 1, "spec.cells after the rejected writes")
	c.NoError(c.cellUntouched(cellKey, cellUID), "Cell after the rejected writes")

	// The normal edit still goes through: the appended cell gets a Cell of its
	// own and the existing Cell is neither replaced nor asked to drain.
	//
	// The wait is on the Cells rather than on the cluster. A second cell also
	// grows the shard's default pool into it, and the suite's data plane does
	// not bring that pool to full strength, so the cluster stays Progressing;
	// that is outside what this test pins.
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Cells = append(
			got.Spec.Cells,
			CellConfig{Name: secondSimCell, ZoneID: "us-central1-b"},
		)
	})
	c.Eventually(30*time.Second, "a Healthy Cell for the appended "+secondSimCell, func() error {
		cells := &multigresv1alpha1.CellList{}
		if err := c.List(
			cells,
			client.MatchingLabels{"multigres.com/cluster": cluster.Name},
		); err != nil {
			return err
		}
		for i := range cells.Items {
			if cells.Items[i].Spec.Name != secondSimCell {
				continue
			}
			if phase := cells.Items[i].Status.Phase; phase != multigresv1alpha1.PhaseHealthy {
				return fmt.Errorf("Cell %s phase is %q", cells.Items[i].Name, phase)
			}
			return nil
		}
		return fmt.Errorf("%d Cells, none for %s", len(cells.Items), secondSimCell)
	})
	c.NoError(c.cellUntouched(cellKey, cellUID), "original Cell after the append")
}

// TestCellsAppendOnly_EmptyListMayBeDropped asserts that a cluster stored with
// an empty spec.cells can still be written by a typed Go client, which omits
// the empty list and so sends an object with no spec.cells at all. Dropping an
// empty list removes no cell, and rejecting it would make every typed update of
// such a cluster fail until someone wrote the empty list back by hand.
//
// The validating webhook refuses a cluster whose shards match no cell, so this
// state exists only with webhooks disabled, which is how the suite runs.
func TestCellsAppendOnly_EmptyListMayBeDropped(t *testing.T) {
	c := newCase(t)
	cluster := c.newCluster("scenario-cells-empty-list", func(spec *MultigresClusterSpec) {
		spec.Cells = nil
	})
	clusterKey := client.ObjectKeyFromObject(cluster)

	got := &MultigresCluster{}
	c.NoError(c.Get(clusterKey, got), "get cluster")
	c.NoError(c.Patch(got, client.RawPatch(types.JSONPatchType,
		[]byte(`[{"op":"add","path":"/spec/cells","value":[]}]`))), "store an empty spec.cells")

	raw := &unstructured.Unstructured{}
	raw.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("MultigresCluster"))
	c.NoError(c.Get(clusterKey, raw), "get cluster as stored")
	cells, found, err := unstructured.NestedSlice(raw.Object, "spec", "cells")
	c.NoError(err, "read stored spec.cells")
	c.True(
		found && len(cells) == 0,
		"want an empty spec.cells stored, got found=%v %v",
		found,
		cells,
	)

	c.updateCluster(cluster, func(got *MultigresCluster) {
		if got.Annotations == nil {
			got.Annotations = map[string]string{}
		}
		got.Annotations["test.multigres.com/touched"] = "true"
	})
}

// onlyCell returns the cluster's single Cell, failing unless there is exactly
// one.
func (c *C) onlyCell(cluster *MultigresCluster) *multigresv1alpha1.Cell {
	c.Helper()
	cells := &multigresv1alpha1.CellList{}
	c.NoError(
		c.List(cells, client.MatchingLabels{"multigres.com/cluster": cluster.Name}),
		"list cells",
	)
	c.Len(cells.Items, 1, "want exactly one Cell")
	return &cells.Items[0]
}

// cellUntouched reports whether the Cell at key is still the one with uid and
// has not been asked to drain.
func (c *C) cellUntouched(key client.ObjectKey, uid types.UID) error {
	cell := &multigresv1alpha1.Cell{}
	if err := c.Get(key, cell); err != nil {
		return err
	}
	if cell.UID != uid {
		return fmt.Errorf("Cell %s was replaced (uid %s, want %s)", key.Name, cell.UID, uid)
	}
	if v := cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion]; v != "" {
		return fmt.Errorf("Cell %s carries PendingDeletion=%q", key.Name, v)
	}
	return nil
}
