package v1alpha1

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// An empty sidecar resources block is left out of the serialised pool, so a
// server-side apply of a pool whose block was cleared removes the field
// instead of applying an empty object over the entries it had.
func TestPoolSpecOmitsEmptyPgBackRestResources(t *testing.T) {
	t.Parallel()

	empty, err := json.Marshal(PoolSpec{})
	require.NoError(t, err)
	require.NotContains(t, string(empty), "pgbackrestResources")

	// A block with an empty map asks for nothing, and is left out once
	// normalised.
	for name, block := range map[string]corev1.ResourceRequirements{
		"empty limits":   {Limits: corev1.ResourceList{}},
		"empty requests": {Requests: corev1.ResourceList{}},
		"empty claims":   {Claims: []corev1.ResourceClaim{}},
	} {
		pool := PoolSpec{PgBackRestResources: block}
		pool.NormalizePgBackRestResources()
		out, err := json.Marshal(pool)
		require.NoError(t, err, name)
		require.NotContains(t, string(out), "pgbackrestResources", name)
	}
	kept := PoolSpec{PgBackRestResources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
	}}
	kept.NormalizePgBackRestResources()
	require.Len(t, kept.PgBackRestResources.Limits, 1)
	// Every kind of entry keeps its block: a block of claims only is still
	// something the user asked for, and validation has to see it.
	for name, block := range map[string]corev1.ResourceRequirements{
		"requests only": {Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		}},
		"limits only": {Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		}},
		"claims only": {Claims: []corev1.ResourceClaim{{Name: "gpu"}}},
	} {
		pool := PoolSpec{PgBackRestResources: block}
		pool.NormalizePgBackRestResources()
		require.Equal(t, block, pool.PgBackRestResources, name)
	}

	set, err := json.Marshal(PoolSpec{PgBackRestResources: corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
	}})
	require.NoError(t, err)
	require.Contains(t, string(set), `"pgbackrestResources":{"limits":{"memory":"1Gi"}}`)
}
