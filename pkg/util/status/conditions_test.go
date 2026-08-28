package status

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
			conditions := make([]metav1.Condition, len(tc.existing))
			copy(conditions, tc.existing)

			SetCondition(&conditions, tc.condition)

			if len(conditions) != len(tc.want) {
				t.Fatalf("got %d conditions, want %d", len(conditions), len(tc.want))
			}
			for i, got := range conditions {
				want := tc.want[i]
				if got.Type != want.Type {
					t.Errorf("[%d] type = %s, want %s", i, got.Type, want.Type)
				}
				if got.Status != want.Status {
					t.Errorf("[%d] status = %s, want %s", i, got.Status, want.Status)
				}
				if got.Reason != want.Reason {
					t.Errorf("[%d] reason = %s, want %s", i, got.Reason, want.Reason)
				}
				if want.LastTransitionTime.IsZero() && !got.LastTransitionTime.IsZero() {
					// The "preserves transition time" case: original was zero,
					// SetCondition should have kept the existing (zero) time.
					if name == "preserves transition time when status unchanged" {
						t.Errorf("[%d] expected preserved zero transition time, got %v",
							i, got.LastTransitionTime)
					}
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
			if got := IsConditionTrue(tc.conditions, tc.condType); got != tc.want {
				t.Errorf("IsConditionTrue() = %v, want %v", got, tc.want)
			}
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
			if got := IsConditionFalse(tc.conditions, tc.condType); got != tc.want {
				t.Errorf("IsConditionFalse() = %v, want %v", got, tc.want)
			}
		})
	}
}
