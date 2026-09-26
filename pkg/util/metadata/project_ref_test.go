package metadata_test

import (
	"testing"

	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func TestResolveProjectRef(t *testing.T) {
	tests := map[string]struct {
		annotations map[string]string
		clusterName string
		want        string
	}{
		"uses explicit project ref": {
			annotations: map[string]string{
				metadata.AnnotationProjectRef: "proj_123",
			},
			clusterName: "cluster-a",
			want:        "proj_123",
		},
		"falls back when annotation missing": {
			annotations: map[string]string{},
			clusterName: "cluster-b",
			want:        "cluster-b",
		},
		"falls back when annotations are nil": {
			clusterName: "cluster-c",
			want:        "cluster-c",
		},
		"falls back when annotation empty": {
			annotations: map[string]string{
				metadata.AnnotationProjectRef: "",
			},
			clusterName: "cluster-d",
			want:        "cluster-d",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := metadata.ResolveProjectRef(tc.annotations, tc.clusterName)
			assert.NewAborting(t).Eq(tc.want, got, "ResolveProjectRef()")
		})
	}
}
