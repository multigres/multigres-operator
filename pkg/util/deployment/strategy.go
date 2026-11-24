// Package deployment holds what the operator's Deployment builders share.
package deployment

import (
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// DefaultStrategy is the strategy the API server stores for a Deployment
// applied without one. A builder sets it rather than leaving the field empty:
// an empty strategy is applied as {}, which then never matches the defaults
// the API server fills in, so ownership.SkipNoopApplies would send the
// Deployment on every pass.
func DefaultStrategy() appsv1.DeploymentStrategy {
	quarter := intstr.FromString("25%")
	return appsv1.DeploymentStrategy{
		Type: appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{
			MaxUnavailable: &quarter,
			MaxSurge:       &quarter,
		},
	}
}
