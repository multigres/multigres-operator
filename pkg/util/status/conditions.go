package status

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SetCondition adds or updates a condition in the slice. When the status
// has not changed, the existing LastTransitionTime is preserved.
func SetCondition(conditions *[]metav1.Condition, condition metav1.Condition) {
	for i, c := range *conditions {
		if c.Type == condition.Type {
			if c.Status != condition.Status {
				(*conditions)[i] = condition
			} else {
				condition.LastTransitionTime = c.LastTransitionTime
				(*conditions)[i] = condition
			}
			return
		}
	}
	*conditions = append(*conditions, condition)
}

// IsConditionTrue returns true if the named condition exists and has status True.
func IsConditionTrue(conditions []metav1.Condition, condType string) bool {
	for _, c := range conditions {
		if c.Type == condType {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}

// IsConditionFalse reports whether the named condition has status False.
func IsConditionFalse(conditions []metav1.Condition, condType string) bool {
	for _, c := range conditions {
		if c.Type == condType {
			return c.Status == metav1.ConditionFalse
		}
	}
	return false
}
