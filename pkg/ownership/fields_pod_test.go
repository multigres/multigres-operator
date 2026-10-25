package ownership_test

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
)

// The fieldsV1 here is encoded from the real condition type, not a literal:
// a copied name once let this row drift from what the shard controller writes.
// It lives in the external test package because shard imports ownership.
func TestFieldsKeyedListPathFormat(t *testing.T) {
	condType := string(shard.PoolerDataReadyCondition)
	written := fieldpath.NewSet(fieldpath.MakePathOrDie(
		"status", "conditions", fieldpath.KeyByFields("type", condType)))
	raw, err := written.ToJSON()
	require.NoError(t, err)

	var set fieldpath.Set
	require.NoError(t, set.FromJSON(bytes.NewReader(raw)))

	var paths []string
	set.Iterate(func(p fieldpath.Path) { paths = append(paths, p.String()) })
	slices.Sort(paths)
	// The type contains '/' and '.', and the fieldpath package quotes it
	// verbatim inside the key, with no escaping.
	require.Equal(t, []string{`.status.conditions[type="multigres.com/pooler-data-ready"]`}, paths)

	row, ok := ownership.OwnerOf("Pod", paths[0])
	require.True(t, ok, "the real pool Pod condition must resolve to a row")
	require.Equal(t, ownership.ManagerShardReadiness, row.Manager)
	require.Equal(t, ownership.Gate, row.Class, "podTopoDrifted and Pod Ready read it back")

	sub, ok := ownership.OwnerOf("Pod", paths[0]+".status")
	require.True(t, ok)
	require.Equal(t, row.Path, sub.Path)
}
