package ownership

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestSyncedEvents walks the passes of one object through each way a
// generation reaches a completed pass.
func TestSyncedEvents(t *testing.T) {
	obj := func(uid types.UID, generation int64) *corev1.ConfigMap {
		return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: "a", Namespace: "ns", UID: uid, Generation: generation,
		}}
	}
	var s SyncedEvents

	// A converged object records nothing.
	s.Observe(obj("u1", 1), 1)
	require.False(t, s.Completed(obj("u1", 1)))

	// A new generation completes on the pass that observes it.
	s.Observe(obj("u1", 2), 1)
	require.True(t, s.Completed(obj("u1", 2)))
	require.False(t, s.Completed(obj("u1", 2)), "once per generation")

	// A pass starting from a cached status older than the completing
	// pass's write observes the generation again and records nothing.
	s.Observe(obj("u1", 2), 1)
	require.False(t, s.Completed(obj("u1", 2)))

	// A generation whose first passes stop early, as a new object or a
	// rollout does, completes on a later pass that sees it already observed.
	s.Observe(obj("u1", 3), 2)
	s.Observe(obj("u1", 3), 3)
	require.True(t, s.Completed(obj("u1", 3)))

	// An object recreated under the same name is a different object.
	s.Observe(obj("u1", 4), 3)
	require.False(t, s.Completed(obj("u2", 4)))

	// A deleted object is forgotten.
	s.Observe(obj("u1", 5), 4)
	s.Forget(types.NamespacedName{Namespace: "ns", Name: "a"})
	require.False(t, s.Completed(obj("u1", 5)))
	require.Empty(t, s.generations)
}
