package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBackingConnectionOf(t *testing.T) {
	obj := &TableGroup{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{AnnotationBackingConnection: "orders-db"},
	}}
	if got := BackingConnectionOf(obj); got != "orders-db" {
		t.Errorf("got %q, want orders-db", got)
	}
	if got := BackingConnectionOf(&TableGroup{}); got != "" {
		t.Errorf("a TableGroup with no annotations is bound to %q", got)
	}
}
