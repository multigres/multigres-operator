package suite

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	cellcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
	"github.com/multigres/multigres-operator/pkg/testutil/golden"
)

// TestGoldenMultigatewayDeployment pilots golden.AssertYAML against
// BuildMultigatewayDeployment, called directly rather than through the
// running cell controller: the builder is exported and reachable from this
// package, so the pilot exercises a pure function with no generated fields to
// strip, rather than an object fetched from envtest.
//
// It builds its own scheme rather than reaching for Suite.Scheme: the
// builder call needs nothing envtest boots, and coupling to Suite would make
// this test pay for the whole suite's startup for no benefit.
//
// The fixture pins both Spec.Observability (via OTEL_EXPORTER_OTLP_ENDPOINT)
// and Spec.Images.Multigateway to fixed values: a golden over a builder's
// output must not depend on values that routine maintenance changes.
func TestGoldenMultigatewayDeployment(t *testing.T) {
	// BuildMultigatewayDeployment resolves OTEL settings from the process
	// environment when Spec.Observability is nil (as it is below), so the
	// golden file is only stable once that read is pinned. "disabled" is the
	// builder's own sentinel for suppressing every OTEL var.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "disabled")

	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "golden-cell",
			Namespace: "default",
			UID:       "golden-cell-uid",
			Labels:    map[string]string{"multigres.com/cluster": "golden-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Multigateway: "info",
			},
			Images: multigresv1alpha1.CellImages{
				Multigateway: multigresv1alpha1.ImageRef(
					"ghcr.io/multigres/multigres:golden-fixture",
				),
			},
		},
	}

	got, err := cellcontroller.BuildMultigatewayDeployment(cell, scheme)
	if err != nil {
		t.Fatalf("BuildMultigatewayDeployment: %v", err)
	}

	golden.AssertYAML(t, got, "testdata/multigateway-deployment.golden.yaml")
}
