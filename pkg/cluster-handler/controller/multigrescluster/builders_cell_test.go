package multigrescluster

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

func TestBuildCell(t *testing.T) {
	scheme := setupScheme()

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Images: multigresv1alpha1.ClusterImages{
				Multigateway: "gateway:latest",
			},
		},
	}

	cellCfg := &multigresv1alpha1.CellConfig{
		Name:   "zone-a",
		ZoneID: "use1-az1",
	}

	gatewaySpec := &multigresv1alpha1.StatelessSpec{
		Replicas: ptr.To(int32(2)),
	}
	noGatewayPlacement := (*multigresv1alpha1.PodPlacementSpec)(nil)

	localTopoSpec := &multigresv1alpha1.LocalTopoServerSpec{} // details not critical for this test
	globalTopoRef := multigresv1alpha1.GlobalTopoServerRef{
		Address: "http://global-etcd:2379",
	}
	allCells := []multigresv1alpha1.CellName{"zone-a", "zone-b"}

	t.Run("Success", func(t *testing.T) {
		got, err := BuildCell(
			cluster,
			cellCfg,
			gatewaySpec,
			noGatewayPlacement,
			localTopoSpec,
			globalTopoRef,
			allCells,
			scheme,
		)
		if err != nil {
			t.Fatalf("BuildCell() error = %v", err)
		}

		// Calculate expected hash: md5("my-cluster", "zone-a") -> "6b6f7386"
		expectedName := name.JoinWithConstraints(name.DefaultConstraints, "my-cluster", "zone-a")
		if got.Name != expectedName {
			t.Errorf("Name = %v, want %v", got.Name, expectedName)
		}
		if got.Spec.ZoneID != "use1-az1" {
			t.Errorf("ZoneID = %v, want %v", got.Spec.ZoneID, "use1-az1")
		}
		if got.Spec.Images.Multigateway != "gateway:latest" {
			t.Errorf("Gateway Image = %v, want %v", got.Spec.Images.Multigateway, "gateway:latest")
		}
		if diff := cmp.Diff(allCells, got.Spec.AllCells); diff != "" {
			t.Errorf("AllCells mismatch (-want +got):\n%s", diff)
		}

		// Verify OwnerReference
		if len(got.OwnerReferences) != 1 {
			t.Errorf("OwnerReferences count = %v, want 1", len(got.OwnerReferences))
		}
	})

	t.Run("Propagates ZoneID", func(t *testing.T) {
		cellCfgWithZoneID := &multigresv1alpha1.CellConfig{
			Name:   "zone-a",
			ZoneID: "use1-az1",
		}
		got, err := BuildCell(
			cluster,
			cellCfgWithZoneID,
			gatewaySpec,
			noGatewayPlacement,
			localTopoSpec,
			globalTopoRef,
			allCells,
			scheme,
		)
		if err != nil {
			t.Fatalf("BuildCell() error = %v", err)
		}
		if got.Spec.ZoneID != "use1-az1" {
			t.Errorf("ZoneID = %v, want use1-az1", got.Spec.ZoneID)
		}
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildCell(
			cluster,
			cellCfg,
			gatewaySpec,
			noGatewayPlacement,
			localTopoSpec,
			globalTopoRef,
			allCells,
			emptyScheme,
		)
		if err == nil {
			t.Error("Expected error due to missing scheme types, got nil")
		}
	})

	t.Run("Propagates explicit project ref annotation", func(t *testing.T) {
		clusterWithProjectRef := cluster.DeepCopy()
		clusterWithProjectRef.Annotations = map[string]string{
			metadata.AnnotationProjectRef: "proj_123",
		}

		got, err := BuildCell(
			clusterWithProjectRef,
			cellCfg,
			gatewaySpec,
			noGatewayPlacement,
			localTopoSpec,
			globalTopoRef,
			allCells,
			scheme,
		)
		if err != nil {
			t.Fatalf("BuildCell() error = %v", err)
		}

		if got.Annotations[metadata.AnnotationProjectRef] != "proj_123" {
			t.Fatalf(
				"annotation %q = %q, want %q",
				metadata.AnnotationProjectRef,
				got.Annotations[metadata.AnnotationProjectRef],
				"proj_123",
			)
		}
	})
}
