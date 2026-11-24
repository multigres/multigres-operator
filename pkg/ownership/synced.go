package ownership

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SyncedEvents decides when a controller records a Synced event: once per
// generation of an object, on the first pass that completes for it. That pass
// is often not the one that first observes the generation: a new object, or a
// change that rolls pods, takes several passes that each stop early and
// requeue, and the status the first of them writes records the generation as
// observed. So the generation a pass observes is remembered until a pass
// completes for it, and then as completed, since the next pass can start from
// a cached status older than the one the completing pass wrote and would
// otherwise observe the same generation again. A pass over an object whose
// status already records its generation, with nothing remembered, records
// nothing, which is what keeps converged passes from writing an event each.
//
// The memory is per process and holds one entry per object that has had a
// generation observed, dropped when the controller finds the object gone. A
// generation observed before a restart and not yet completed records no
// Synced, since its status already reads as observed, and in exchange a
// restart does not record one for every object.
//
// The zero value is ready to use.
type SyncedEvents struct {
	mu          sync.Mutex
	generations map[types.NamespacedName]syncedGeneration
}

type syncedGeneration struct {
	uid        types.UID
	generation int64
	completed  bool
}

// Observe remembers obj's generation when its status, which records
// observedGeneration, has not yet observed it, unless it is remembered
// already. Call it with the object as read at the start of a pass, before the
// pass writes status.
func (s *SyncedEvents) Observe(obj client.Object, observedGeneration int64) {
	if observedGeneration == obj.GetGeneration() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generations == nil {
		s.generations = map[types.NamespacedName]syncedGeneration{}
	}
	key := client.ObjectKeyFromObject(obj)
	if g, ok := s.generations[key]; ok && g.uid == obj.GetUID() &&
		g.generation == obj.GetGeneration() {
		return
	}
	s.generations[key] = syncedGeneration{uid: obj.GetUID(), generation: obj.GetGeneration()}
}

// Completed reports whether a pass that completed for obj records Synced: its
// generation is remembered as observed and no pass has completed for it yet.
func (s *SyncedEvents) Completed(obj client.Object) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := client.ObjectKeyFromObject(obj)
	g, ok := s.generations[key]
	if !ok || g.completed || g.uid != obj.GetUID() || g.generation != obj.GetGeneration() {
		return false
	}
	g.completed = true
	s.generations[key] = g
	return true
}

// Forget drops what is remembered of the object at key, which no longer
// exists.
func (s *SyncedEvents) Forget(key types.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.generations, key)
}
