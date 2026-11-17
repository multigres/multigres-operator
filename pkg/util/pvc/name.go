package pvc

import (
	"strconv"
	"strings"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// PoolDataName is the PostgreSQL claim mounted at a given pool ordinal.
func PoolDataName(shard *multigresv1alpha1.Shard, pool, cell string, index int) string {
	base := name.JoinWithConstraints(
		name.PodConstraints,
		append(ShardStorageIdentity(shard), "pool", pool, cell)...)
	return "data-" + base + "-" + strconv.Itoa(index)
}

// ShardStorageIdentity is the name parts that identify a shard's volumes:
// cluster, database, tablegroup, shard, and for a shard of a dynamic
// tablegroup its catalog id. Retained PVCs outlive their Shard under the same
// name, and a pool reuses an existing PVC by name, so without the id a
// tablegroup recreated under a dropped one's name (a new catalog id, a new
// backup scope) would start on the dropped one's data directory and backup
// volume. Spec-declared tablegroups have no id and keep their names.
func ShardStorageIdentity(shard *multigresv1alpha1.Shard) []string {
	parts := []string{
		shard.Labels["multigres.com/cluster"],
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
	}
	if id := multigresv1alpha1.TableGroupIDOf(shard); id > 0 {
		parts = append(parts, strconv.FormatInt(id, 10))
	}
	return append(parts, string(shard.Spec.ShardName))
}

// Ordinal identifies the first claim in the same replica set and the ordinal
// for a data PVC name ending in the canonical decimal pod index.
func Ordinal(claim string) (first string, index int, ok bool) {
	pos := strings.LastIndexByte(claim, '-')
	if pos < 0 {
		return "", 0, false
	}
	ordinal := claim[pos+1:]
	i, err := strconv.Atoi(ordinal)
	if err != nil || i < 0 || strconv.Itoa(i) != ordinal {
		return "", 0, false
	}
	return claim[:pos+1] + "0", i, true
}
