package suite

import (
	"fmt"
	"math"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestEtcdSimIsIdempotentAndFactoryRefusesUnregistered(t *testing.T) {
	c := newCase(t)
	a := c.EtcdSim("topo", 3)
	if b := c.EtcdSim("topo", 3); a != b {
		t.Fatal("second EtcdSim call returned a different cluster")
	}
	ts := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Namespace: c.NS, Name: "topo"},
	}
	cl, err := etcdSimFactory(t.Context(), ts)
	if err != nil {
		t.Fatalf("registered TopoServer: %v", err)
	}
	cl.Close()

	other := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Namespace: c.NS, Name: "other"},
	}
	_, err = etcdSimFactory(t.Context(), other)
	if want := "no etcd sim registered for " + c.NS + "/other"; err == nil || err.Error() != want {
		t.Fatalf("unregistered: got %v, want %q", err, want)
	}

	ts.Spec.TLS = &multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)}
	if _, err := etcdSimFactory(
		t.Context(),
		ts,
	); err == nil ||
		!strings.Contains(err.Error(), "answers on") {
		t.Fatalf("endpoint mismatch not reported: %v", err)
	}
}

func TestEtcdSimRejectsOutOfRangeMembers(t *testing.T) {
	c := newCase(t)
	for _, members := range []int{0, -1, math.MaxInt32 + 1} {
		func() {
			defer func() {
				want := fmt.Sprintf("members %d out of range", members)
				if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), want) {
					t.Fatalf("members=%d: recovered %v, want panic containing %q", members, r, want)
				}
			}()
			c.EtcdSim("bad", members)
		}()
	}
}
