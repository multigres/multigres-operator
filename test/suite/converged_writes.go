package suite

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/multigres/testkit/ctrltest"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// opSummary renders ops as sorted "controller verb kind name: count" lines.
func opSummary(ops []ctrltest.Op) string {
	counts := map[string]int{}
	for _, op := range ops {
		counts[fmt.Sprintf("%s %s %s %s", op.Controller, op.Verb, op.Kind, op.Key.Name)]++
	}
	lines := make([]string, 0, len(counts))
	for k, n := range counts {
		lines = append(lines, fmt.Sprintf("%s: %d", k, n))
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n  ")
}

// capturedEvents holds, for each namespace a measurement is watching, the
// events the controllers asked to record there, before the recorder's own
// spam filter, which drops an object's events once it has recorded a burst of
// them. Reading Events back from the API server would miss whatever that
// filter dropped, which is most of an object's events after the burst of its
// first reconciles.
var capturedEvents struct {
	sync.Mutex
	byNamespace map[string][]string
}

// captureEvents starts recording the events in the case's namespace and
// returns a function that stops and returns them, as "controller kind name
// reason: message" lines. A case that ends before calling it stops the
// recording at cleanup.
func (c *C) captureEvents() func() []string {
	capturedEvents.Lock()
	defer capturedEvents.Unlock()
	if capturedEvents.byNamespace == nil {
		capturedEvents.byNamespace = map[string][]string{}
	}
	capturedEvents.byNamespace[c.NS] = []string{}
	c.Cleanup(func() {
		capturedEvents.Lock()
		defer capturedEvents.Unlock()
		delete(capturedEvents.byNamespace, c.NS)
	})
	return func() []string {
		capturedEvents.Lock()
		defer capturedEvents.Unlock()
		lines := capturedEvents.byNamespace[c.NS]
		delete(capturedEvents.byNamespace, c.NS)
		return lines
	}
}

// loggingRecorder captures each event and passes it on.
type loggingRecorder struct {
	record.EventRecorder
	controller string
}

func (r loggingRecorder) log(obj runtime.Object, reason, message string) {
	o, ok := obj.(client.Object)
	if !ok {
		return
	}
	capturedEvents.Lock()
	defer capturedEvents.Unlock()
	lines, ok := capturedEvents.byNamespace[o.GetNamespace()]
	if !ok {
		return
	}
	capturedEvents.byNamespace[o.GetNamespace()] = append(lines, fmt.Sprintf("%s %T %s %s: %s",
		r.controller, obj, o.GetName(), reason, message))
}

func (r loggingRecorder) Event(obj runtime.Object, eventtype, reason, message string) {
	r.log(obj, reason, message)
	r.EventRecorder.Event(obj, eventtype, reason, message)
}

func (r loggingRecorder) Eventf(
	obj runtime.Object,
	eventtype, reason, messageFmt string,
	args ...any,
) {
	r.log(obj, reason, fmt.Sprintf(messageFmt, args...))
	r.EventRecorder.Eventf(obj, eventtype, reason, messageFmt, args...)
}

func (r loggingRecorder) AnnotatedEventf(
	obj runtime.Object,
	annotations map[string]string,
	eventtype, reason, messageFmt string,
	args ...any,
) {
	r.log(obj, reason, fmt.Sprintf(messageFmt, args...))
	r.EventRecorder.AnnotatedEventf(obj, annotations, eventtype, reason, messageFmt, args...)
}
