package testutil

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/testkit/assert"
)

func TestFindLatestEventFor(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		events    []ResourceEvent
		setupObj  func() client.Object
		wantFound bool
		wantName  string
		wantNS    string
		wantKind  string
	}{
		"empty namespace matches all": {
			events: []ResourceEvent{
				{Kind: "Service", Name: "svc1", Namespace: "default"},
				{Kind: "Service", Name: "svc1", Namespace: "kube-system"},
				{Kind: "Service", Name: "svc2", Namespace: "default"},
			},
			setupObj: func() client.Object {
				obj := &corev1.Service{}
				obj.SetName("svc1")
				obj.SetNamespace("") // Empty namespace
				return obj
			},
			wantFound: true,
			wantName:  "svc1",
			wantNS:    "kube-system", // Should return the latest svc1 event
			wantKind:  "Service",
		},
		"empty name matches all in namespace": {
			events: []ResourceEvent{
				{Kind: "Service", Name: "svc1", Namespace: "default"},
				{Kind: "Service", Name: "svc2", Namespace: "default"},
				{Kind: "Deployment", Name: "deploy1", Namespace: "default"},
			},
			setupObj: func() client.Object {
				obj := &corev1.Service{}
				obj.SetName("") // Empty name
				obj.SetNamespace("default")
				return obj
			},
			wantFound: true,
			wantKind:  "Service",
			wantName:  "svc2", // Should return the latest Service in default namespace
			wantNS:    "default",
		},
		"kind mismatch returns nil": {
			events: []ResourceEvent{
				{Kind: "Service", Name: "svc1", Namespace: "default"},
			},
			setupObj: func() client.Object {
				obj := &corev1.ConfigMap{}
				obj.SetName("svc1")
				obj.SetNamespace("default")
				return obj
			},
			wantFound: false,
		},
		"name mismatch returns nil": {
			events: []ResourceEvent{
				{Kind: "Service", Name: "svc-a", Namespace: "ns-a"},
				{Kind: "Service", Name: "svc-b", Namespace: "ns-b"},
			},
			setupObj: func() client.Object {
				obj := &corev1.Service{}
				obj.SetName("svc-nonexist")
				obj.SetNamespace("ns-a")
				return obj
			},
			wantFound: false,
		},
		"namespace mismatch returns nil": {
			events: []ResourceEvent{
				{Kind: "Service", Name: "svc-a", Namespace: "ns-a"},
				{Kind: "Service", Name: "svc-b", Namespace: "ns-b"},
			},
			setupObj: func() client.Object {
				obj := &corev1.Service{}
				obj.SetName("svc-a")
				obj.SetNamespace("ns-nonexist")
				return obj
			},
			wantFound: false,
		},
		"exact match found": {
			events: []ResourceEvent{
				{Kind: "Service", Name: "svc-a", Namespace: "ns-a"},
				{Kind: "Service", Name: "svc-b", Namespace: "ns-b"},
				{Kind: "Pod", Name: "pod-a", Namespace: "ns-a"},
			},
			setupObj: func() client.Object {
				obj := &corev1.Service{}
				obj.SetName("svc-b")
				obj.SetNamespace("ns-b")
				return obj
			},
			wantFound: true,
			wantName:  "svc-b",
			wantNS:    "ns-b",
			wantKind:  "Service",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)

			watcher := &ResourceWatcher{
				t:      t,
				events: tc.events,
			}

			evt := watcher.findLatestEventFor(tc.setupObj())

			c.Require().
				Eq(tc.wantFound, (evt != nil), "findLatestEventFor() found=%v, want=", evt != nil)

			if !tc.wantFound {
				return
			}

			c.Eq(tc.wantName, evt.Name, "findLatestEventFor() Name")
			c.Eq(tc.wantNS, evt.Namespace, "findLatestEventFor() Namespace")
			c.Eq(tc.wantKind, evt.Kind, "findLatestEventFor() Kind")
		})
	}
}

func TestFindLatestEvent(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		events    []ResourceEvent
		matchFunc func(e ResourceEvent) bool
		wantFound bool
	}{
		"no match returns nil": {
			events: []ResourceEvent{
				{Kind: "Service", Name: "svc1", Namespace: "default"},
				{Kind: "Service", Name: "svc2", Namespace: "default"},
			},
			matchFunc: func(e ResourceEvent) bool {
				return e.Name == "nonexistent"
			},
			wantFound: false,
		},
		"match found": {
			events: []ResourceEvent{
				{Kind: "Service", Name: "svc1", Namespace: "default"},
				{Kind: "Service", Name: "svc2", Namespace: "default"},
			},
			matchFunc: func(e ResourceEvent) bool {
				return e.Name == "svc1"
			},
			wantFound: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			watcher := &ResourceWatcher{
				t:      t,
				events: tc.events,
			}

			evt := watcher.findLatestEvent(tc.matchFunc)

			assert.NewCollecting(t).
				Eq(tc.wantFound, (evt != nil), "findLatestEvent() found=%v, want=", evt != nil)
		})
	}
}

func TestCheckLatestEventMatches(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		events      []ResourceEvent
		setupObj    func() client.Object
		wantMatched bool
		wantDiff    string
	}{
		"no event exists": {
			events: []ResourceEvent{},
			setupObj: func() client.Object {
				svc := &corev1.Service{}
				svc.SetName("nonexistent")
				svc.SetNamespace("default")
				return svc
			},
			wantMatched: false,
			wantDiff:    "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)

			watcher := &ResourceWatcher{
				t:      t,
				events: tc.events,
			}

			matched, diff := watcher.checkLatestEventMatches(tc.setupObj(), nil)

			c.Eq(tc.wantMatched, matched, "checkLatestEventMatches() matched")
			c.Eq(tc.wantDiff, diff, "checkLatestEventMatches() diff")
		})
	}
}
