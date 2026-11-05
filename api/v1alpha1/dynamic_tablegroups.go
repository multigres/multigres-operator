package v1alpha1

import (
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Dynamic tablegroups are tablegroups created with CREATE TABLEGROUP through
// a gateway rather than declared in spec.databases[].tablegroups. Multigres's
// sharding catalog owns which of them exist; the operator reads that list from
// multiadmin and provisions a TableGroup per entry.
//
// The opt-in and the per-incarnation catalog id are carried as annotations, not
// spec fields: an annotation needs no CRD change, so this works against a
// cluster whose installed CRDs predate the feature.
const (
	// AnnotationDynamicTableGroups opts a MultigresCluster into dynamic
	// tablegroups when set to "true". The operator copies it onto the
	// cluster's Cells, whose gateways then run with --enable-tablegroup-ddl.
	AnnotationDynamicTableGroups = "multigres.com/dynamic-tablegroups"

	// AnnotationTableGroupID records the sharding catalog id of a dynamic
	// tablegroup on its TableGroup and Shard. It identifies one incarnation of
	// a name: a tablegroup dropped and created again under the same name gets
	// a new id. The Shard controller passes it to poolers as --tablegroup-id.
	AnnotationTableGroupID = "multigres.com/tablegroup-id"

	// LabelDynamicTableGroup marks TableGroups and Shards the operator
	// provisioned from the sharding catalog, as opposed to spec-declared ones.
	LabelDynamicTableGroup = "multigres.com/dynamic-tablegroup"

	// ConditionDynamicTableGroups reports, on a MultigresCluster, whether the
	// last read of desired tablegroups succeeded and what is provisioned.
	ConditionDynamicTableGroups = "DynamicTableGroups"

	// ConditionDynamicTableGroupConflict is True on a MultigresCluster while
	// the catalog lists a tablegroup whose name the spec also declares. The
	// spec-declared tablegroup is left alone.
	ConditionDynamicTableGroupConflict = "DynamicTableGroupConflict"
)

// DynamicTableGroupsEnabled reports whether obj carries the dynamic
// tablegroups opt-in.
func DynamicTableGroupsEnabled(obj metav1.Object) bool {
	return obj.GetAnnotations()[AnnotationDynamicTableGroups] == "true"
}

// TableGroupIDOf returns the catalog id recorded on a dynamic TableGroup or
// Shard, or 0 when obj carries none or an unparsable one.
func TableGroupIDOf(obj metav1.Object) int64 {
	id, err := strconv.ParseInt(obj.GetAnnotations()[AnnotationTableGroupID], 10, 64)
	if err != nil || id <= 0 {
		return 0
	}
	return id
}
