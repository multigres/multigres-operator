package status

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/multigres/testkit/assert"
)

func TestSetCondition(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		existing  []metav1.Condition
		condition metav1.Condition
		want      []metav1.Condition
	}{
		"appends to empty slice": {
			existing: nil,
			condition: metav1.Condition{
				Type:    "Ready",
				Status:  metav1.ConditionTrue,
				Reason:  "AllGood",
				Message: "everything is fine",
			},
			want: []metav1.Condition{
				{
					Type:    "Ready",
					Status:  metav1.ConditionTrue,
					Reason:  "AllGood",
					Message: "everything is fine",
				},
			},
		},
		"appends new type": {
			existing: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllGood"},
			},
			condition: metav1.Condition{
				Type: "Available", Status: metav1.ConditionFalse, Reason: "NotYet",
			},
			want: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllGood"},
				{Type: "Available", Status: metav1.ConditionFalse, Reason: "NotYet"},
			},
		},
		"updates status transition": {
			existing: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "NotReady",
					LastTransitionTime: metav1.Now(),
				},
			},
			condition: metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "AllGood",
				LastTransitionTime: metav1.Now(),
			},
			want: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllGood"},
			},
		},
		"preserves transition time when status unchanged": {
			existing: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "AllGood",
					LastTransitionTime: metav1.Time{},
				},
			},
			condition: metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "StillGood",
				LastTransitionTime: metav1.Now(),
			},
			want: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "StillGood",
					LastTransitionTime: metav1.Time{},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)
			conditions := make([]metav1.Condition, len(tc.existing))
			copy(conditions, tc.existing)

			SetCondition(&conditions, tc.condition)

			c.Require().Len(conditions, len(tc.want), "got %d conditions, want", len(conditions))
			for i, got := range conditions {
				want := tc.want[i]
				c.Eq(want.Type, got.Type, "[%d] type = %s, want", i, got.Type)
				c.Eq(want.Status, got.Status, "[%d] status = %s, want", i, got.Status)
				c.Eq(want.Reason, got.Reason, "[%d] reason = %s, want", i, got.Reason)
				if want.LastTransitionTime.IsZero() && !got.LastTransitionTime.IsZero() {
					// The "preserves transition time" case: original was zero,
					// SetCondition should have kept the existing (zero) time.
					c.NotEq(
						"preserves transition time when status unchanged",
						name,
						"[%d] expected preserved zero transition time, got %v",
						i,
						got.LastTransitionTime,
					)
				}
			}
		})
	}
}

func TestIsConditionTrue(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		conditions []metav1.Condition
		condType   string
		want       bool
	}{
		"true when present and True": {
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
			},
			condType: "Ready",
			want:     true,
		},
		"false when present and False": {
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionFalse},
			},
			condType: "Ready",
			want:     false,
		},
		"false when present and Unknown": {
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionUnknown},
			},
			condType: "Ready",
			want:     false,
		},
		"false when not present": {
			conditions: []metav1.Condition{
				{Type: "Available", Status: metav1.ConditionTrue},
			},
			condType: "Ready",
			want:     false,
		},
		"false on empty slice": {
			conditions: nil,
			condType:   "Ready",
			want:       false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.NewCollecting(t).
				Eq(tc.want, IsConditionTrue(tc.conditions, tc.condType), "IsConditionTrue()")
		})
	}
}

func TestIsConditionFalse(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		conditions []metav1.Condition
		condType   string
		want       bool
	}{
		"true when present and False": {
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionFalse},
			},
			condType: "Ready",
			want:     true,
		},
		"false when present and True": {
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
			},
			condType: "Ready",
			want:     false,
		},
		"false when present and Unknown": {
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionUnknown},
			},
			condType: "Ready",
			want:     false,
		},
		"false when not present": {
			conditions: []metav1.Condition{
				{Type: "Available", Status: metav1.ConditionFalse},
			},
			condType: "Ready",
			want:     false,
		},
		"false on empty slice": {
			conditions: nil,
			condType:   "Ready",
			want:       false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.NewCollecting(t).
				Eq(tc.want, IsConditionFalse(tc.conditions, tc.condType), "IsConditionFalse()")
		})
	}
}
