package pvc

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

func TestBuildRetentionPolicy(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		policy      *multigresv1alpha1.PVCDeletionPolicy
		wantDeleted appsv1.PersistentVolumeClaimRetentionPolicyType
		wantScaled  appsv1.PersistentVolumeClaimRetentionPolicyType
	}{
		"nil policy defaults to Retain/Retain": {
			policy:      nil,
			wantDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			wantScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		},
		"Delete/Delete": {
			policy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
			wantDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			wantScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		},
		"Retain/Delete": {
			policy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
			wantDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			wantScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		},
		"Delete/Retain": {
			policy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
			},
			wantDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			wantScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		},
		"unknown values default to Retain": {
			policy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: "SomethingUnknown",
				WhenScaled:  "AnotherUnknown",
			},
			wantDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			wantScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)
			got := BuildRetentionPolicy(tc.policy)
			c.Eq(tc.wantDeleted, got.WhenDeleted, "WhenDeleted")
			c.Eq(tc.wantScaled, got.WhenScaled, "WhenScaled")
		})
	}
}
