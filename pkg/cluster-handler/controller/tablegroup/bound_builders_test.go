package tablegroup

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestBuildShard_BoundTableGroup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "c-postgres-legacy", Namespace: "default", UID: "tg-uid",
			Labels: map[string]string{
				"multigres.com/cluster":                  "c",
				multigresv1alpha1.LabelDynamicTableGroup: "true",
			},
			Annotations: map[string]string{
				multigresv1alpha1.AnnotationTableGroupID:      "9",
				multigresv1alpha1.AnnotationBackingConnection: "orders-db",
			},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   "postgres",
			TableGroupName: "legacy",
		},
	}
	shard, err := BuildShard(tg, &multigresv1alpha1.ShardResolvedSpec{Name: "0-inf"}, scheme)
	if err != nil {
		t.Fatal(err)
	}
	if got := multigresv1alpha1.BackingConnectionOf(shard); got != "orders-db" {
		t.Errorf("backing connection = %q, want orders-db", got)
	}
	if got := multigresv1alpha1.TableGroupIDOf(shard); got != 9 {
		t.Errorf("tablegroup id = %d, want 9", got)
	}

	// A managed dynamic tablegroup's Shard does not carry one.
	delete(tg.Annotations, multigresv1alpha1.AnnotationBackingConnection)
	shard, err = BuildShard(tg, &multigresv1alpha1.ShardResolvedSpec{Name: "0-inf"}, scheme)
	if err != nil {
		t.Fatal(err)
	}
	if multigresv1alpha1.BackingConnectionOf(shard) != "" {
		t.Errorf("managed shard got bound metadata: %v", shard.Annotations)
	}
}
