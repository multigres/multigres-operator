package multigrescluster

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"

	"github.com/multigres/testkit/assert"
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

	gatewaySpec := &multigresv1alpha1.MultigatewaySpec{
		StatelessSpec: multigresv1alpha1.StatelessSpec{
			Replicas: ptr.To(int32(2)),
		},
	}
	noGatewayPlacement := (*multigresv1alpha1.PodPlacementSpec)(nil)

	localTopoSpec := &multigresv1alpha1.LocalTopoServerSpec{} // details not critical for this test
	globalTopoRef := multigresv1alpha1.GlobalTopoServerRef{
		Address: "http://global-etcd:2379",
	}
	allCells := []multigresv1alpha1.CellName{"zone-a", "zone-b"}

	t.Run("Success", func(t *testing.T) {
		c := assert.NewCollecting(t)
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
		c.Require().NoError(err, "BuildCell() error =")

		// Calculate expected hash: md5("my-cluster", "zone-a") -> "6b6f7386"
		expectedName := name.JoinWithConstraints(name.DefaultConstraints, "my-cluster", "zone-a")
		c.Eq(expectedName, got.Name, "Name")
		c.Eq("use1-az1", got.Spec.ZoneID, "ZoneID")
		c.Eq("gateway:latest", got.Spec.Images.Multigateway, "Gateway Image")
		c.EqDiff(allCells, got.Spec.AllCells, "AllCells mismatch")

		// Verify OwnerReference
		c.Len(
			got.OwnerReferences,
			1,
			"OwnerReferences count = %v, want 1",
			len(got.OwnerReferences),
		)
	})

	t.Run("Propagates ZoneID", func(t *testing.T) {
		c := assert.NewCollecting(t)
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
		c.Require().NoError(err, "BuildCell() error =")
		c.Eq("use1-az1", got.Spec.ZoneID, "ZoneID")
	})

	t.Run("Propagates InternalTLS", func(t *testing.T) {
		c := assert.NewAborting(t)
		clusterWithInternalTLS := cluster.DeepCopy()
		clusterWithInternalTLS.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{
			Enabled: ptr.To(true),
		}

		got, err := BuildCell(
			clusterWithInternalTLS,
			cellCfg,
			gatewaySpec,
			noGatewayPlacement,
			localTopoSpec,
			globalTopoRef,
			allCells,
			scheme,
		)
		c.NoError(err, "BuildCell() error =")
		c.Eq(clusterWithInternalTLS.Spec.InternalTLS, got.Spec.InternalTLS, "InternalTLS")
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
		assert.NewCollecting(t).Error(err, "Expected error due to missing scheme types, got nil")
	})

	t.Run("Propagates explicit project ref annotation", func(t *testing.T) {
		c := assert.NewAborting(t)
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
		c.NoError(err, "BuildCell() error =")

		c.Eq(
			"proj_123",
			got.Annotations[metadata.AnnotationProjectRef],
			"annotation %q = %q, want",
			metadata.AnnotationProjectRef,
			got.Annotations[metadata.AnnotationProjectRef],
		)
	})
}
