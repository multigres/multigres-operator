package ownership_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
)

// The Pod row must match the path the apiserver records for the condition the
// shard controller writes, so the path is built from the real condition type
// rather than a literal. It lives in the external test package because shard
// imports ownership.
func TestFieldsKeyedListPathFormat(t *testing.T) {
	written := fieldpath.MakePathOrDie(
		"status",
		"conditions",
		fieldpath.KeyByFields("type", string(shard.PoolerDataReadyCondition)),
	).String()
	// The type contains '/' and '.', and the fieldpath package quotes it
	// verbatim inside the key, with no escaping.
	require.Equal(t, `.status.conditions[type="multigres.com/pooler-data-ready"]`, written)

	row, ok := ownership.OwnerOf("Pod", written)
	require.True(t, ok, "the real pool Pod condition must resolve to a row")
	require.Equal(t, ownership.ManagerShardReadiness, row.Manager)

	sub, ok := ownership.OwnerOf("Pod", written+".status")
	require.True(t, ok)
	require.Equal(t, row.Path, sub.Path)
}
