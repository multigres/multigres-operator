package observer

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestGlobalTopologyRoot(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		cluster *multigresv1alpha1.MultigresCluster
		objects []client.Object
		want    string
	}{
		"uses canonical project root by default": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "cluster",
					Namespace:   "default",
					Annotations: map[string]string{metadata.AnnotationProjectRef: "project-123"},
				},
			},
			want: "/multigres/project-123/global",
		},
		"resolves root from core template": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{CoreTemplate: "shared"},
				},
			},
			objects: []client.Object{&multigresv1alpha1.CoreTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default"},
				Spec: multigresv1alpha1.CoreTemplateSpec{
					GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{RootPath: "/custom/global"},
					},
				},
			}},
			want: "/custom/global",
		},
		"inline root overrides template": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{RootPath: "/inline/global"},
					},
				},
			},
			want: "/inline/global",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			scheme := runtime.NewScheme()
			if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			o := &Observer{client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build()}

			got, err := o.globalTopologyRoot(t.Context(), tc.cluster)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("globalTopologyRoot() = %q, want %q", got, tc.want)
			}
		})
	}
}
