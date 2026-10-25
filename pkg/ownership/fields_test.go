package ownership

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestFieldsCoverEveryStatusField(t *testing.T) {
	statuses := map[string]reflect.Type{
		kindCluster:    reflect.TypeFor[multigresv1alpha1.MultigresClusterStatus](),
		kindTableGroup: reflect.TypeFor[multigresv1alpha1.TableGroupStatus](),
		kindCell:       reflect.TypeFor[multigresv1alpha1.CellStatus](),
		kindTopoServer: reflect.TypeFor[multigresv1alpha1.TopoServerStatus](),
		kindShard:      reflect.TypeFor[multigresv1alpha1.ShardStatus](),
	}
	for kind, typ := range statuses {
		for i := range typ.NumField() {
			name, _, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
			require.NotEmpty(t, name, "%s.%s has no json name", kind, typ.Field(i).Name)
			_, ok := OwnerOf(kind, ".status."+name)
			require.True(t, ok, "%s .status.%s has no ownership row", kind, name)
		}
	}
}

func TestFieldsWellFormed(t *testing.T) {
	kinds := []string{kindCluster, kindTableGroup, kindCell, kindTopoServer, kindShard, kindPod}
	seen := map[[2]string]bool{}
	for _, f := range Fields {
		id := f.Kind + " " + f.Path
		require.Contains(t, kinds, f.Kind, id)
		require.NotEmpty(t, f.Path, id)
		require.True(
			t,
			strings.HasPrefix(f.Path, ".status"),
			"%s: path must start with .status",
			id,
		)
		require.Contains(t, Managers(), f.Manager, id)
		require.Contains(t, []Class{Observation, Gate, WorkflowMemory}, f.Class, id)
		require.False(t, seen[[2]string{f.Kind, f.Path}], "%s: duplicate row", id)
		seen[[2]string{f.Kind, f.Path}] = true
	}
}
