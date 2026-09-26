package resolver

import (
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"k8s.io/utils/ptr"

	"github.com/multigres/testkit/assert"
)

func TestMergeEtcdMaintenance(t *testing.T) {
	base := &multigresv1alpha1.EtcdSpec{Maintenance: &multigresv1alpha1.EtcdMaintenanceConfig{
		AutoCompactionMode:      "periodic",
		AutoCompactionRetention: "6h",
		QuotaBackendBytes:       ptr.To(int64(1 << 30)),
		DefragmentationEnabled:  ptr.To(true),
	}}
	override := &multigresv1alpha1.EtcdSpec{
		Maintenance: &multigresv1alpha1.EtcdMaintenanceConfig{
			DefragmentationEnabled: ptr.To(false),
			QuotaBackendBytes:      ptr.To(int64(512 << 20)),
		},
	}
	mergeEtcdSpec(base, override)
	if base.Maintenance.AutoCompactionRetention != "6h" ||
		base.Maintenance.DefragmentationIsEnabled() ||
		base.Maintenance.EffectiveQuotaBackendBytes() != 512<<20 {
		t.Fatalf("incorrect merge: %+v", base.Maintenance)
	}
	*override.Maintenance.DefragmentationEnabled = true
	*override.Maintenance.QuotaBackendBytes = 2 << 30
	assert.NewAborting(t).False(base.Maintenance.DefragmentationIsEnabled() ||
		base.Maintenance.EffectiveQuotaBackendBytes() != 512<<20, "merged maintenance aliases override")
}
